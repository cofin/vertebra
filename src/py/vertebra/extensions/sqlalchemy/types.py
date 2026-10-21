"""Cross-database Vector and BinaryJson column types for SQLAlchemy 2.1 and Vertebra."""

from __future__ import annotations

import array
import struct
from typing import TYPE_CHECKING, Any, Literal

import msgspec

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "BinaryJson",
    "Vector",
    "VectorDistanceExpr",
)

type VectorStorageFormat = Literal["float32", "float64", "int8", "binary"]
type VectorDistanceMetric = Literal["cosine", "l2", "inner_product"]


class VectorDistanceExpr:
    """Dialect-aware SQL expression descriptor for vector similarity operations."""

    def __init__(
        self,
        column_name: str,
        query_vector: Sequence[float],
        *,
        metric: VectorDistanceMetric = "cosine",
        param_name: str = "query_vec",
    ) -> None:
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

    def __init__(
        self,
        dim: int | None = None,
        *,
        storage_format: VectorStorageFormat = "float32",
        column_name: str = "embedding",
    ) -> None:
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
        """Decode a database vector result into a Python ``list[float]``."""
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
    """Cross-database binary JSON column descriptor (`JSONB` on PG and SQLite 3.45+, OSON `JSON` on Oracle 26ai)."""

    cache_ok: bool = True

    def get_col_spec(self, dialect_name: str = "postgresql") -> str:
        """Render the DDL column type specification for the target dialect."""
        base = dialect_name.split("+", 1)[0].lower()
        if base in ("postgres", "postgresql", "sqlite"):
            return "JSONB"
        return "JSON"

    def process_bind_param(self, value: Any, dialect_name: str = "postgresql") -> bytes | str | None:
        """Encode a Python value into UTF-8 JSON via ``msgspec``."""
        _ = dialect_name
        if value is None:
            return None
        if isinstance(value, (bytes, str)):
            return value
        return msgspec.json.encode(value).decode("utf-8")

    def process_result_value(self, value: Any, dialect_name: str = "postgresql") -> Any:
        """Decode a JSON/JSONB/OSON payload via ``msgspec``."""
        _ = dialect_name
        if value is None or isinstance(value, (dict, list)):
            return value
        if isinstance(value, memoryview):
            raw = bytes(value)
            if raw.startswith(b"\x01"):
                raw = raw[1:]
            return msgspec.json.decode(raw)
        if isinstance(value, (bytes, bytearray)):
            raw_b = bytes(value)
            if raw_b.startswith(b"\x01"):
                raw_b = raw_b[1:]
            return msgspec.json.decode(raw_b)
        if isinstance(value, str):
            return msgspec.json.decode(value.encode("utf-8"))
        return value
