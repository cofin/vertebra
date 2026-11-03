"""SQLAlchemy 2.1 custom types (Vector and BinaryJSON) and ORM query optimization descriptors."""

from __future__ import annotations

import array
import struct
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

import msgspec
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import JSON, NVARCHAR, TypeDecorator, UserDefinedType

from vertebra import _vertebra
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect
    from sqlalchemy.sql.type_api import TypeEngine

__all__ = (
    "BinaryJSON",
    "DeleteUsingClause",
    "InsertSentinelSpec",
    "ParameterizedSQL",
    "SelectInLoadSpec",
    "Vector",
    "VectorDistanceExpr",
    "VectorDistanceMetric",
    "VectorStorageFormat",
)

type VectorStorageFormat = Literal["float32", "float64", "int8", "binary"]
type VectorDistanceMetric = Literal["cosine", "l2", "inner_product"]


class VectorDistanceExpr:
    """Dialect-aware SQL expression descriptor for vector similarity operations."""

    __slots__ = ("_approximate", "_column_name", "_metric", "_num_leaves_to_search", "_param_name", "_query_vector")

    def __init__(
        self,
        column_name: str,
        query_vector: Sequence[float],
        *,
        metric: VectorDistanceMetric = "cosine",
        param_name: str = "query_vec",
        approximate: bool = False,
        num_leaves_to_search: int | None = None,
    ) -> None:
        """Initialize vector distance expression with target column, vector, and metric."""
        self._column_name = column_name
        self._query_vector = tuple(float(v) for v in query_vector)
        self._metric = metric
        self._param_name = param_name
        self._approximate = approximate
        self._num_leaves_to_search = num_leaves_to_search

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

    @property
    def approximate(self) -> bool:
        """Return True when compiling an approximate ANN distance function."""
        return self._approximate

    def compile_sql(self, dialect_name: str = "postgresql") -> str:
        """Compile the vector distance expression for the target database dialect."""
        lowered = dialect_name.lower()
        base = lowered.split("+", 1)[0]
        if base == "spangres":
            op = {
                "cosine": "<=>",
                "l2": "<->",
                "inner_product": "<#>",
            }[self._metric]
            return f"{self._column_name} {op} :{self._param_name}"
        if base in ("bigquery", "spanner"):
            if self._approximate and base == "spanner":
                approx_fn = {
                    "cosine": "APPROX_COSINE_DISTANCE",
                    "l2": "APPROX_EUCLIDEAN_DISTANCE",
                    "inner_product": "APPROX_DOT_PRODUCT",
                }[self._metric]
                if self._num_leaves_to_search is not None:
                    return (
                        f"{approx_fn}({self._column_name}, @{self._param_name}, "
                        f"options => JSON '{{\"num_leaves_to_search\": {int(self._num_leaves_to_search)}}}')"
                    )
                return f"{approx_fn}({self._column_name}, @{self._param_name})"
            gcp_fn = {
                "cosine": "COSINE_DISTANCE",
                "l2": "EUCLIDEAN_DISTANCE",
                "inner_product": "DOT_PRODUCT",
            }[self._metric]
            return f"{gcp_fn}({self._column_name}, @{self._param_name})"
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


class Vector(UserDefinedType[Any]):
    """Dense vector column type supporting PostgreSQL (pgvector), Oracle 26ai, MySQL 9.0, SQLite, MSSQL, and DuckDB."""

    cache_ok: bool = True
    __slots__ = ("binary_format", "column_name", "dimensions", "precision")

    def __init__(
        self,
        dimensions: int | None = None,
        *,
        precision: VectorStorageFormat = "float32",
        column_name: str = "embedding",
        dim: int | None = None,
        storage_format: VectorStorageFormat | None = None,
        binary_format: bool = False,
    ) -> None:
        """Initialize dense vector column type with dimensions, precision, and column name."""
        resolved_dim = dim if dim is not None else dimensions
        resolved_prec = storage_format if storage_format is not None else precision
        self.dimensions = resolved_dim
        self.precision = resolved_prec
        self.column_name = column_name
        self.binary_format = binary_format

    @property
    def dim(self) -> int | None:
        """Return dimensions for backward compatibility with existing tests."""
        return self.dimensions

    @property
    def storage_format(self) -> VectorStorageFormat:
        """Return storage precision format."""
        return self.precision

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render the DDL column type specification for the target dialect."""
        target_name = ""
        if isinstance(dialect_name, str):
            target_name = dialect_name
        elif "dialect" in kw:
            target_name = getattr(kw["dialect"], "name", "")
        lowered = target_name.lower() if target_name else "postgresql"
        base = lowered.split("+", 1)[0]
        if base == "spangres":
            return f"VECTOR({self.dimensions})" if self.dimensions is not None else "VECTOR"

        if base == "bigquery":
            return "ARRAY<FLOAT64>"
        if base == "spanner":
            elem_type = "FLOAT64" if self.precision == "float64" else "FLOAT32"
            if self.dimensions is not None:
                return f"ARRAY<{elem_type}>(vector_length=>{self.dimensions})"
            return f"ARRAY<{elem_type}>"
        if base == "oracle":
            ora_fmt = {
                "float32": "FLOAT32",
                "float64": "FLOAT64",
                "int8": "INT8",
                "binary": "BINARY",
            }.get(self.precision, "FLOAT32")
            if self.dimensions is None:
                return f"VECTOR(*, {ora_fmt})"
            return f"VECTOR({self.dimensions}, {ora_fmt})"
        if base in ("postgres", "postgresql", "mysql", "mariadb", "duckdb", "mssql"):
            return f"VECTOR({self.dimensions})" if self.dimensions is not None else "VECTOR"
        if base == "sqlite":
            return "BLOB"
        return f"VECTOR({self.dimensions})" if self.dimensions is not None else "VECTOR"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return a processor callable for binding Python float lists to DBAPI parameters."""
        dname = getattr(dialect, "name", "postgresql")

        def _process(value: Any) -> Any:
            return self.process_bind_param(value, dialect_name=dname)

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return a processor callable for decoding database result values into Python float lists."""
        _ = coltype
        dname = getattr(dialect, "name", "postgresql")

        def _process(value: Any) -> list[float] | None:
            return self.process_result_value(value, dialect_name=dname)

        return _process

    def process_bind_param(
        self,
        value: Any,
        dialect_name: str = "postgresql",
    ) -> Any:
        """Convert a Python float sequence or VectorView into a dialect-optimal bind representation."""
        if value is None:
            return None
        base = dialect_name.split("+", 1)[0].lower()
        typecode = "d" if self.precision == "float64" else "f"

        if isinstance(value, _vertebra.VectorView):
            if base in ("bigquery", "spanner"):
                return [float(x) for x in value.to_list()]
            if base == "oracle":
                if self.binary_format:
                    return value.to_oracle_bytes()
                return array.array(typecode, value.to_list())
            if base == "sqlite":
                return value.to_sqlite_blob()
            if base in ("postgres", "postgresql") and self.binary_format:
                return value.to_pgvector_bytes()
            return "[" + ",".join(str(float(x)) for x in value.to_list()) + "]"

        if base in ("bigquery", "spanner"):
            return [float(x) for x in value]
        if base == "oracle":
            if self.binary_format:
                return _vertebra.encode_oracle_vector_f32(value)
            if isinstance(value, array.array):
                return value
            return array.array(typecode, [float(x) for x in value])
        if base == "sqlite":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return bytes(value)
            arr = value if isinstance(value, array.array) else array.array(typecode, [float(x) for x in value])
            return arr.tobytes()
        if base in ("postgres", "postgresql") and self.binary_format:
            return _vertebra.encode_pgvector_f32(value)
        return "[" + ",".join(str(float(x)) for x in value) + "]"

    def process_result_value(
        self,
        value: Any,
        dialect_name: str = "postgresql",
    ) -> list[float] | None:
        """Decode a database vector result into a Python list of floats."""
        if value is None:
            return None
        if isinstance(value, _vertebra.VectorView):
            return value.to_list()
        if isinstance(value, array.array):
            return list(value)
        if isinstance(value, list):
            return [float(x) for x in value]
        if isinstance(value, (bytes, bytearray, memoryview)):
            raw = bytes(value)
            base = dialect_name.split("+", 1)[0].lower()
            if base == "oracle" and len(raw) >= 8 and raw[0] == 0xDB:
                return _vertebra.decode_oracle_vector_f32(raw)
            if base in ("postgres", "postgresql") and len(raw) >= 4:
                dim, unused = struct.unpack(">HH", raw[:4])
                if unused == 0 and len(raw) == 4 + dim * 4:
                    return _vertebra.decode_pgvector_f32(raw)
            count = len(raw) // 4
            return list(struct.unpack(f"<{count}f", raw))
        if isinstance(value, str):
            trimmed = value.strip().lstrip("[").rstrip("]")
            if not trimmed:
                return []
            try:
                decoded = decode_json(f"[{trimmed}]")
                if isinstance(decoded, list):
                    return [float(x) for x in decoded]
            except Exception:
                pass
            return [float(item) for item in trimmed.split(",") if item.strip()]
        return list(value)

    def cosine_distance(self, query_vector: Sequence[float], *, param_name: str = "query_vec") -> VectorDistanceExpr:
        """Build a cosine distance expression for this vector column."""
        return VectorDistanceExpr(self.column_name, query_vector, metric="cosine", param_name=param_name)

    def approx_cosine_distance(
        self,
        query_vector: Sequence[float],
        *,
        param_name: str = "query_vec",
        num_leaves_to_search: int | None = None,
    ) -> VectorDistanceExpr:
        """Build an approximate cosine distance expression (`APPROX_COSINE_DISTANCE` on Spanner ScaNN)."""
        return VectorDistanceExpr(
            self.column_name,
            query_vector,
            metric="cosine",
            param_name=param_name,
            approximate=True,
            num_leaves_to_search=num_leaves_to_search,
        )

    def l2_distance(self, query_vector: Sequence[float], *, param_name: str = "query_vec") -> VectorDistanceExpr:
        """Build an L2 / Euclidean distance expression for this vector column."""
        return VectorDistanceExpr(self.column_name, query_vector, metric="l2", param_name=param_name)

    def inner_product(self, query_vector: Sequence[float], *, param_name: str = "query_vec") -> VectorDistanceExpr:
        """Build an inner-product / dot-product expression for this vector column."""
        return VectorDistanceExpr(self.column_name, query_vector, metric="inner_product", param_name=param_name)


@compiles(Vector, "bigquery")
def _compile_vector_bigquery(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (element, compiler, kw)
    return "ARRAY<FLOAT64>"


@compiles(Vector, "spanner")
def _compile_vector_spanner(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    elem_type = "FLOAT64" if element.precision == "float64" else "FLOAT32"
    if element.dimensions is not None:
        return f"ARRAY<{elem_type}>(vector_length=>{element.dimensions})"
    return f"ARRAY<{elem_type}>"


@compiles(Vector, "postgresql")
def _compile_vector_postgresql(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    return f"VECTOR({element.dimensions})" if element.dimensions is not None else "VECTOR"


@compiles(Vector, "oracle")
def _compile_vector_oracle(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    ora_fmt = {
        "float32": "FLOAT32",
        "float64": "FLOAT64",
        "int8": "INT8",
        "binary": "BINARY",
    }.get(element.precision, "FLOAT32")
    if element.dimensions is None:
        return f"VECTOR(*, {ora_fmt})"
    return f"VECTOR({element.dimensions}, {ora_fmt})"


@compiles(Vector, "mysql")
def _compile_vector_mysql(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    return f"VECTOR({element.dimensions})" if element.dimensions is not None else "VECTOR"


@compiles(Vector, "sqlite")
def _compile_vector_sqlite(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (element, compiler, kw)
    return "BLOB"


@compiles(Vector, "duckdb")
def _compile_vector_duckdb(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    return f"VECTOR({element.dimensions})" if element.dimensions is not None else "VECTOR"


@compiles(Vector, "mssql")
def _compile_vector_mssql(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    return f"VECTOR({element.dimensions})" if element.dimensions is not None else "VECTOR"


class BinaryJSON(TypeDecorator[Any]):
    """Cross-database JSON/JSONB column type (JSONB on PostgreSQL/CockroachDB, JSON on others, NVARCHAR on MSSQL)."""

    impl = JSON
    cache_ok: bool = True
    __slots__ = ()

    def load_dialect_impl(self, dialect: Dialect | Any) -> TypeEngine[Any]:
        """Select the dialect-native JSON type implementation."""
        name = getattr(dialect, "name", "").lower()
        if name in ("postgresql", "cockroachdb", "spangres"):
            return dialect.type_descriptor(JSONB())
        if name == "mssql":
            return dialect.type_descriptor(NVARCHAR(None))
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: Any, dialect: Dialect | Any) -> Any:
        """Encode Python structures to JSON strings or bytes."""
        _ = dialect
        if value is None or isinstance(value, (bytes, str)):
            return value
        return encode_json(value)

    def process_result_value(self, value: Any, dialect: Dialect | Any) -> Any:
        """Decode database JSON payloads into Python objects."""
        _ = dialect
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

    def get_col_spec(self, dialect_name: str = "postgresql", **kw: Any) -> str:
        """Render the DDL column type specification for dialect-compatibility inspection."""
        target = dialect_name
        if not target and "dialect" in kw:
            target = getattr(kw["dialect"], "name", "")
        lowered = target.lower() if target else "postgresql"
        base = lowered.split("+", 1)[0]
        if base in ("postgres", "postgresql", "spangres", "sqlite"):
            return "JSONB"
        if base == "mssql":
            return "NVARCHAR(MAX)"
        return "JSON"


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
        """Configure SQLAlchemy 2.1 selectinload with chunksize and omit_join=True."""
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


class ParameterizedSQL(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Compiled parameterized SQL statement produced from a PEP 750 / SQLAlchemy 2.1 template."""

    sql: str
    params: dict[str, Any]

    @classmethod
    def from_template(
        cls,
        template: Any,
        parts: Sequence[Any] | None = None,
        *,
        param_prefix: str = "p",
    ) -> ParameterizedSQL:
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
