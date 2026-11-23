"""SQLAlchemy 2.1 custom vector, spatial, and JSON types with Comparator expressions."""

from __future__ import annotations

import array
import struct
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import Float, literal
from sqlalchemy.dialects import mssql, oracle, postgresql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.operators import OperatorClass
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.types import JSON, TypeDecorator, UserDefinedType

from vertebra import _vertebra
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect
    from sqlalchemy.sql.compiler import SQLCompiler
    from sqlalchemy.sql.type_api import TypeEngine

__all__ = (
    "BigQueryGeography",
    "HalfVector",
    "MsgspecJSON",
    "OracleVector",
    "PgVector",
    "SpannerVector",
    "SparseVector",
    "Vector",
    "VectorDistanceMetric",
    "VectorStorageFormat",
)

type VectorStorageFormat = Literal["float32", "float64", "int8", "binary"]
type VectorDistanceMetric = Literal["cosine", "l2", "inner_product"]

_PGVECTOR_OPERATORS: dict[str, str] = {
    "cosine": "<=>",
    "l2": "<->",
    "l1": "<+>",
    "inner_product": "<#>",
}

_ORACLE_METRICS: dict[str, str] = {
    "cosine": "COSINE",
    "l2": "EUCLIDEAN",
    "l1": "MANHATTAN",
    "inner_product": "DOT",
}

_MYSQL_DISTANCE_FUNCTIONS: dict[str, str] = {
    "cosine": "DISTANCE_COSINE",
    "l2": "DISTANCE_EUCLIDEAN",
    "inner_product": "DISTANCE_DOT",
}

_GCP_DISTANCE_FUNCTIONS: dict[str, str] = {
    "cosine": "COSINE_DISTANCE",
    "l2": "EUCLIDEAN_DISTANCE",
    "inner_product": "DOT_PRODUCT",
}

_SPANNER_APPROX_FUNCTIONS: dict[str, str] = {
    "cosine": "APPROX_COSINE_DISTANCE",
    "l2": "APPROX_EUCLIDEAN_DISTANCE",
    "inner_product": "APPROX_DOT_PRODUCT",
}


class _VectorDistance(ColumnElement[float]):
    """Dialect-deferred vector distance expression returning a float column element."""

    inherit_cache = True
    _traverse_internals = [
        ("left", InternalTraversal.dp_clauseelement),
        ("right", InternalTraversal.dp_clauseelement),
        ("metric", InternalTraversal.dp_string),
        ("approximate", InternalTraversal.dp_boolean),
        ("options_json", InternalTraversal.dp_string),
    ]

    def __init__(
        self,
        left: ColumnElement[Any],
        right: ColumnElement[Any],
        metric: str,
        *,
        approximate: bool = False,
        options_json: str = "",
    ) -> None:
        """Initialize a dialect-deferred vector distance AST node."""
        self.left = left
        self.right = right
        self.metric = metric
        self.approximate = approximate
        self.options_json = options_json
        self.type = Float()


class _VectorComparator(UserDefinedType.Comparator[Any]):
    """Dialect-aware vector distance operators for similarity search."""

    def _distance(
        self,
        other: Sequence[float] | ColumnElement[Any],
        metric: str,
        *,
        approximate: bool = False,
        options_json: str = "",
    ) -> _VectorDistance:
        operand: ColumnElement[Any] = (
            other if isinstance(other, ColumnElement) else literal([float(v) for v in other], type_=self.type)
        )
        return _VectorDistance(
            self.expr,
            operand,
            metric,
            approximate=approximate,
            options_json=options_json,
        )

    def cosine_distance(self, other: Sequence[float] | ColumnElement[Any]) -> _VectorDistance:
        """Build a cosine distance SQL expression."""
        return self._distance(other, "cosine")

    def l2_distance(self, other: Sequence[float] | ColumnElement[Any]) -> _VectorDistance:
        """Build an L2 Euclidean distance SQL expression."""
        return self._distance(other, "l2")

    def l1_distance(self, other: Sequence[float] | ColumnElement[Any]) -> _VectorDistance:
        """Build an L1 Manhattan distance SQL expression."""
        return self._distance(other, "l1")

    def inner_product(self, other: Sequence[float] | ColumnElement[Any]) -> _VectorDistance:
        """Build an inner-product distance SQL expression."""
        return self._distance(other, "inner_product")

    def max_inner_product(self, other: Sequence[float] | ColumnElement[Any]) -> _VectorDistance:
        """Build a maximum inner-product SQL expression."""
        return self._distance(other, "inner_product")

    def approx_cosine_distance(
        self,
        other: Sequence[float] | ColumnElement[Any],
        *,
        num_leaves_to_search: int | None = None,
    ) -> _VectorDistance:
        """Build an approximate cosine distance SQL expression."""
        options_json = (
            encode_json({"num_leaves_to_search": int(num_leaves_to_search)}) if num_leaves_to_search is not None else ""
        )
        return self._distance(
            other,
            "cosine",
            approximate=True,
            options_json=options_json,
        )


def _decode_text_vector(value: str) -> list[float]:
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


class Vector(UserDefinedType[Any]):
    """Dense vector column type supporting PostgreSQL, Spangres, Oracle 23ai, MySQL 9.0, SQLite, MSSQL, BigQuery, Spanner, and DuckDB."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    Comparator = _VectorComparator
    comparator_factory = _VectorComparator
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
        self.precision: VectorStorageFormat = resolved_prec
        self.column_name = column_name
        self.binary_format = binary_format

    @property
    def dim(self) -> int | None:
        """Return dimensions."""
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
            if base in ("postgres", "postgresql", "spangres") and self.binary_format:
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
        if base in ("postgres", "postgresql", "spangres") and self.binary_format:
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
            if base in ("postgres", "postgresql", "spangres") and len(raw) >= 4:
                dim, unused = struct.unpack(">HH", raw[:4])
                if unused == 0 and len(raw) == 4 + dim * 4:
                    return _vertebra.decode_pgvector_f32(raw)
            count = len(raw) // 4
            return list(struct.unpack(f"<{count}f", raw))
        if isinstance(value, str):
            return _decode_text_vector(value)
        return list(value)


class PgVector(UserDefinedType[Any]):
    """PostgreSQL pgvector dense vector column type."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    comparator_factory = _VectorComparator
    __slots__ = ("column_name", "dimensions")

    def __init__(
        self,
        dimensions: int | None = None,
        *,
        dim: int | None = None,
        column_name: str = "embedding",
    ) -> None:
        """Initialize PostgreSQL pgvector column type with dimensions and column name."""
        self.dimensions = dim if dim is not None else dimensions
        self.column_name = column_name

    @property
    def dim(self) -> int | None:
        """Return dimensions."""
        return self.dimensions

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render VECTOR DDL specification."""
        _ = (dialect_name, kw)
        if self.dimensions is not None:
            return f"VECTOR({self.dimensions})"
        return "VECTOR"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return bind processor converting sequence of floats to pgvector string."""
        _ = dialect

        def _process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return "[" + ",".join(str(float(x)) for x in value.to_list()) + "]"
            if isinstance(value, (list, tuple)):
                return "[" + ",".join(str(float(x)) for x in value) + "]"
            if isinstance(value, str):
                return value
            return "[" + ",".join(str(float(x)) for x in value) + "]"

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return result processor converting pgvector result to list of floats."""
        _ = (dialect, coltype)

        def _process(value: Any) -> list[float] | None:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return value.to_list()
            if isinstance(value, (list, tuple)):
                return [float(x) for x in value]
            if isinstance(value, (bytes, bytearray, memoryview)):
                raw = bytes(value)
                if len(raw) >= 4:
                    dim_count, unused = struct.unpack(">HH", raw[:4])
                    if unused == 0 and len(raw) == 4 + dim_count * 4:
                        return _vertebra.decode_pgvector_f32(raw)
                count = len(raw) // 4
                return list(struct.unpack(f"<{count}f", raw))
            if isinstance(value, str):
                return _decode_text_vector(value)
            return [float(x) for x in value]

        return _process


class HalfVector(UserDefinedType[Any]):
    """PostgreSQL pgvector 16-bit halfvec column type."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    comparator_factory = _VectorComparator
    __slots__ = ("column_name", "dimensions")

    def __init__(
        self,
        dimensions: int | None = None,
        *,
        dim: int | None = None,
        column_name: str = "embedding",
    ) -> None:
        """Initialize halfvec column type with dimensions and column name."""
        self.dimensions = dim if dim is not None else dimensions
        self.column_name = column_name

    @property
    def dim(self) -> int | None:
        """Return dimensions."""
        return self.dimensions

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render HALFVEC DDL specification."""
        _ = (dialect_name, kw)
        if self.dimensions is not None:
            return f"HALFVEC({self.dimensions})"
        return "HALFVEC"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return bind processor converting sequence of floats to halfvec string."""
        _ = dialect

        def _process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return "[" + ",".join(str(float(x)) for x in value.to_list()) + "]"
            if isinstance(value, (list, tuple)):
                return "[" + ",".join(str(float(x)) for x in value) + "]"
            if isinstance(value, str):
                return value
            return "[" + ",".join(str(float(x)) for x in value) + "]"

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return result processor converting halfvec result to list of floats."""
        _ = (dialect, coltype)

        def _process(value: Any) -> list[float] | None:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return value.to_list()
            if isinstance(value, (list, tuple)):
                return [float(x) for x in value]
            if isinstance(value, str):
                return _decode_text_vector(value)
            return [float(x) for x in value]

        return _process


class SparseVector(UserDefinedType[Any]):
    """PostgreSQL pgvector sparsevec column type."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    comparator_factory = _VectorComparator
    __slots__ = ("column_name", "dimensions")

    def __init__(
        self,
        dimensions: int | None = None,
        *,
        dim: int | None = None,
        column_name: str = "embedding",
    ) -> None:
        """Initialize sparsevec column type with dimensions and column name."""
        self.dimensions = dim if dim is not None else dimensions
        self.column_name = column_name

    @property
    def dim(self) -> int | None:
        """Return dimensions."""
        return self.dimensions

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render SPARSEVEC DDL specification."""
        _ = (dialect_name, kw)
        if self.dimensions is not None:
            return f"SPARSEVEC({self.dimensions})"
        return "SPARSEVEC"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return bind processor for sparse vectors."""
        _ = dialect

        def _process(value: Any) -> Any:
            if value is None or isinstance(value, str):
                return value
            return str(value)

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return result processor for sparse vectors."""
        _ = (dialect, coltype)

        def _process(value: Any) -> Any:
            return value

        return _process


class OracleVector(UserDefinedType[Any]):
    """Oracle 23ai native VECTOR column type."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    comparator_factory = _VectorComparator
    __slots__ = ("binary_format", "column_name", "dimensions", "precision")

    def __init__(
        self,
        dimensions: int | None = None,
        *,
        precision: VectorStorageFormat = "float32",
        column_name: str = "embedding",
        dim: int | None = None,
        binary_format: bool = False,
    ) -> None:
        """Initialize Oracle VECTOR column type with dimensions, precision format, and column name."""
        self.dimensions = dim if dim is not None else dimensions
        self.precision = precision
        self.column_name = column_name
        self.binary_format = binary_format

    @property
    def dim(self) -> int | None:
        """Return dimensions."""
        return self.dimensions

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render Oracle VECTOR DDL specification."""
        _ = (dialect_name, kw)
        ora_fmt = self.precision.upper()
        if self.dimensions is None:
            return f"VECTOR(*, {ora_fmt})"
        return f"VECTOR({self.dimensions}, {ora_fmt})"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return bind processor converting floats to array.array or binary bytes."""
        _ = dialect
        typecode = "d" if self.precision == "float64" else "f"

        def _process(value: Any) -> Any:
            if value is None:
                return None
            if self.binary_format:
                if isinstance(value, _vertebra.VectorView):
                    return value.to_oracle_bytes()
                return _vertebra.encode_oracle_vector_f32(value)
            if isinstance(value, array.array):
                return value
            if isinstance(value, _vertebra.VectorView):
                return array.array(typecode, value.to_list())
            if isinstance(value, (list, tuple)):
                return array.array(typecode, [float(x) for x in value])
            return value

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return result processor converting Oracle vector bytes/arrays to list of floats."""
        _ = (dialect, coltype)

        def _process(value: Any) -> list[float] | None:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return value.to_list()
            if isinstance(value, array.array):
                return list(value)
            if isinstance(value, (list, tuple)):
                return [float(x) for x in value]
            if isinstance(value, (bytes, bytearray, memoryview)):
                raw = bytes(value)
                if len(raw) >= 8 and raw[0] == 0xDB:
                    return _vertebra.decode_oracle_vector_f32(raw)
                count = len(raw) // 4
                return list(struct.unpack(f"<{count}f", raw))
            return [float(x) for x in value]

        return _process


class SpannerVector(UserDefinedType[Any]):
    """Google Cloud Spanner ARRAY<FLOAT32> vector column type."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    comparator_factory = _VectorComparator
    __slots__ = ("column_name", "dimensions", "precision")

    def __init__(
        self,
        dimensions: int | None = None,
        *,
        precision: VectorStorageFormat = "float32",
        column_name: str = "embedding",
        dim: int | None = None,
    ) -> None:
        """Initialize Cloud Spanner vector column type with length, precision, and column name."""
        self.dimensions = dim if dim is not None else dimensions
        self.precision = precision
        self.column_name = column_name

    @property
    def dim(self) -> int | None:
        """Return dimensions."""
        return self.dimensions

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render Spanner ARRAY DDL specification."""
        _ = (dialect_name, kw)
        elem_type = "FLOAT64" if self.precision == "float64" else "FLOAT32"
        if self.dimensions is not None:
            return f"ARRAY<{elem_type}>(vector_length=>{self.dimensions})"
        return f"ARRAY<{elem_type}>"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return bind processor converting float sequence to list."""
        _ = dialect

        def _process(value: Any) -> list[float] | None:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return [float(x) for x in value.to_list()]
            return [float(x) for x in value]

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return result processor converting Spanner result to list of floats."""
        _ = (dialect, coltype)

        def _process(value: Any) -> list[float] | None:
            if value is None:
                return None
            if isinstance(value, _vertebra.VectorView):
                return value.to_list()
            return [float(x) for x in value]

        return _process


class BigQueryGeography(UserDefinedType[Any]):
    """Google BigQuery GEOGRAPHY column type for spatial data."""

    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    __slots__ = ()

    def get_col_spec(self, dialect_name: str | None = None, **kw: Any) -> str:
        """Render GEOGRAPHY DDL specification."""
        _ = (dialect_name, kw)
        return "GEOGRAPHY"

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return bind processor converting spatial payload to WKT string."""
        _ = dialect

        def _process(value: Any) -> str | None:
            if value is None:
                return None
            if isinstance(value, str):
                return value
            if isinstance(value, (bytes, bytearray)):
                return bytes(value).decode("utf-8")
            return str(value)

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return result processor converting GEOGRAPHY to WKT string."""
        _ = (dialect, coltype)

        def _process(value: Any) -> str | None:
            if value is None:
                return None
            if isinstance(value, str):
                return value
            if isinstance(value, (bytes, bytearray)):
                return bytes(value).decode("utf-8")
            return str(value)

        return _process


class MsgspecJSON(TypeDecorator[Any]):
    """Cross-database JSON/JSONB column type serialized through centralized msgspec encode/decode."""

    impl = JSON
    cache_ok: bool | None = True
    operator_classes = OperatorClass.ANY
    __slots__ = ()

    def load_dialect_impl(self, dialect: Dialect | Any) -> TypeEngine[Any]:
        """Select the dialect-native JSON type implementation."""
        name = getattr(dialect, "name", "").lower()
        if name in ("postgresql", "cockroachdb", "spangres"):
            return dialect.type_descriptor(postgresql.JSONB())
        if name == "sqlite":
            return dialect.type_descriptor(sqlite.JSONB())
        if name == "mssql":
            return dialect.type_descriptor(mssql.NVARCHAR(None))
        if name == "oracle":
            sv = getattr(dialect, "server_version_info", None)
            if sv is not None and sv[0] < 21:
                return dialect.type_descriptor(oracle.JSON(use_blob=True))
            return dialect.type_descriptor(oracle.JSON())
        return dialect.type_descriptor(JSON())

    def bind_processor(self, dialect: Dialect | Any) -> Any:
        """Return a processor calling process_bind_param without double-encoding via sqltypes.JSON."""

        def _process(value: Any) -> Any:
            return self.process_bind_param(value, dialect)

        return _process

    def result_processor(self, dialect: Dialect | Any, coltype: Any) -> Any:
        """Return a processor calling process_result_value directly on DBAPI results."""
        _ = coltype

        def _process(value: Any) -> Any:
            return self.process_result_value(value, dialect)

        return _process

    def process_bind_param(self, value: Any, dialect: Dialect | Any) -> Any:
        """Encode Python values including str to JSON strings using encode_json."""
        _ = dialect
        if value is None or isinstance(value, bytes):
            return value
        return encode_json(value)

    def process_result_value(self, value: Any, dialect: Dialect | Any) -> Any:
        """Decode database JSON payloads into Python objects using decode_json."""
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
        """Render the DDL column type specification for dialect inspection."""
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


@compiles(_VectorDistance)
def _compile_vector_distance_unsupported(element: _VectorDistance, compiler: SQLCompiler, **kw: Any) -> str:
    _ = (element, kw)
    dialect_name = getattr(compiler.dialect, "name", "unknown")
    msg = (
        f"Vector distance operations require a native vector backend; "
        f"dialect {dialect_name!r} has no native distance operator."
    )
    raise NotImplementedError(msg)


@compiles(_VectorDistance, "postgresql")
@compiles(_VectorDistance, "spangres")
def _compile_vector_distance_postgresql(element: _VectorDistance, compiler: SQLCompiler, **kw: Any) -> str:
    op = _PGVECTOR_OPERATORS.get(element.metric)
    if op is None:
        msg = f"Unsupported vector distance metric for pgvector: {element.metric!r}"
        raise NotImplementedError(msg)
    return f"({compiler.process(element.left, **kw)} {op} {compiler.process(element.right, **kw)})"


@compiles(_VectorDistance, "oracle")
def _compile_vector_distance_oracle(element: _VectorDistance, compiler: SQLCompiler, **kw: Any) -> str:
    metric = _ORACLE_METRICS.get(element.metric)
    if metric is None:
        msg = f"Unsupported vector distance metric for Oracle 23ai: {element.metric!r}"
        raise NotImplementedError(msg)
    return f"VECTOR_DISTANCE({compiler.process(element.left, **kw)}, {compiler.process(element.right, **kw)}, {metric})"


@compiles(_VectorDistance, "mysql")
def _compile_vector_distance_mysql(element: _VectorDistance, compiler: SQLCompiler, **kw: Any) -> str:
    fn = _MYSQL_DISTANCE_FUNCTIONS.get(element.metric)
    if fn is None:
        msg = f"Unsupported vector distance metric for MySQL: {element.metric!r}"
        raise NotImplementedError(msg)
    return f"{fn}({compiler.process(element.left, **kw)}, {compiler.process(element.right, **kw)})"


@compiles(_VectorDistance, "bigquery")
def _compile_vector_distance_bigquery(element: _VectorDistance, compiler: SQLCompiler, **kw: Any) -> str:
    fn = _GCP_DISTANCE_FUNCTIONS.get(element.metric)
    if fn is None or element.approximate:
        msg = f"Unsupported vector distance metric for BigQuery: {element.metric!r}"
        raise NotImplementedError(msg)
    return f"{fn}({compiler.process(element.left, **kw)}, {compiler.process(element.right, **kw)})"


@compiles(_VectorDistance, "spanner")
def _compile_vector_distance_spanner(element: _VectorDistance, compiler: SQLCompiler, **kw: Any) -> str:
    left_sql = compiler.process(element.left, **kw)
    right_sql = compiler.process(element.right, **kw)
    if element.approximate:
        approx_fn = _SPANNER_APPROX_FUNCTIONS.get(element.metric)
        if approx_fn is None:
            msg = f"Unsupported approximate vector distance metric for Cloud Spanner: {element.metric!r}"
            raise NotImplementedError(msg)
        if element.options_json:
            return f"{approx_fn}({left_sql}, {right_sql}, options => JSON '{element.options_json}')"
        return f"{approx_fn}({left_sql}, {right_sql})"
    fn = _GCP_DISTANCE_FUNCTIONS.get(element.metric)
    if fn is None:
        msg = f"Unsupported vector distance metric for Cloud Spanner: {element.metric!r}"
        raise NotImplementedError(msg)
    return f"{fn}({left_sql}, {right_sql})"


@compiles(Vector, "bigquery")
def _compile_vector_bigquery(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (element, compiler, kw)
    return "ARRAY<FLOAT64>"


@compiles(Vector, "postgresql")
@compiles(Vector, "spangres")
@compiles(Vector, "mysql")
@compiles(Vector, "duckdb")
@compiles(Vector, "mssql")
@compiles(PgVector, "postgresql")
@compiles(PgVector, "spangres")
def _compile_vector_dim(element: Vector | PgVector, compiler: Any, **kw: Any) -> str:
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
    return f"VECTOR(*, {ora_fmt})" if element.dimensions is None else f"VECTOR({element.dimensions}, {ora_fmt})"


@compiles(Vector, "sqlite")
def _compile_vector_sqlite(element: Vector, compiler: Any, **kw: Any) -> str:
    _ = (element, compiler, kw)
    return "BLOB"


@compiles(HalfVector, "postgresql")
def _compile_halfvector(element: HalfVector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    return f"HALFVEC({element.dimensions})" if element.dimensions is not None else "HALFVEC"


@compiles(SparseVector, "postgresql")
def _compile_sparsevector(element: SparseVector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    return f"SPARSEVEC({element.dimensions})" if element.dimensions is not None else "SPARSEVEC"


@compiles(OracleVector, "oracle")
def _compile_oracle_vector(element: OracleVector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    ora_fmt = element.precision.upper()
    return f"VECTOR(*, {ora_fmt})" if element.dimensions is None else f"VECTOR({element.dimensions}, {ora_fmt})"


@compiles(Vector, "spanner")
@compiles(SpannerVector, "spanner")
def _compile_spanner_vector(element: Vector | SpannerVector, compiler: Any, **kw: Any) -> str:
    _ = (compiler, kw)
    elem_type = "FLOAT64" if element.precision == "float64" else "FLOAT32"
    if element.dimensions is not None:
        return f"ARRAY<{elem_type}>(vector_length=>{element.dimensions})"
    return f"ARRAY<{elem_type}>"


@compiles(BigQueryGeography, "bigquery")
def _compile_bq_geography(element: BigQueryGeography, compiler: Any, **kw: Any) -> str:
    _ = (element, compiler, kw)
    return "GEOGRAPHY"
