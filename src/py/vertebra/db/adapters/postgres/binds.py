"""PostgreSQL bind compiler bridge and immutable parameter normalizer."""

from __future__ import annotations

import copy
import functools
from collections.abc import Buffer, Callable, Mapping, Sequence
from typing import Any, Final

import msgspec

from vertebra._vertebra import VectorBatchView, VectorView
from vertebra.db.adapters.postgres.types import (
    PgArray,
    PgComposite,
    PgJson,
    PgParameter,
    PgUnchangedToast,
    PgVector,
)
from vertebra.db.driver import ProgrammingError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json

__all__ = (
    "PG_JSONB_TEXT",
    "CompiledPostgresBinds",
    "compile_postgres_binds",
    "normalize_postgres_parameter",
    "normalize_postgres_parameters",
)

PG_JSONB_TEXT: Final[int] = 3802
_PRIMITIVE_PARAM_TYPES: Final[frozenset[type]] = frozenset((int, str, float, bool, bytes, type(None)))


class CompiledPostgresBinds(msgspec.Struct, frozen=True):
    """Result of single-pass PostgreSQL bind compilation and parameter normalization."""

    sql: str
    params: tuple[Any, ...]
    style: str


@functools.cache
def _resolve_native_compile_binds() -> Callable[
    [str, Sequence[Any] | Mapping[str, Any] | None],
    tuple[str, tuple[Any, ...]],
]:
    """Resolve and cache ``vertebra._postgres.compile_binds``."""
    native_mod: Any = ensure_module(
        "vertebra._postgres",
        package="vertebra-core[postgres]",
        extra="postgres",
    )
    compile_binds: Callable[
        [str, Sequence[Any] | Mapping[str, Any] | None],
        tuple[str, tuple[Any, ...]],
    ] = native_mod.compile_binds
    return compile_binds


def normalize_postgres_parameter(value: Any) -> Any:
    """Convert mutable/FFI parameter wrappers into immutable owned PostgreSQL values.

    Copies mutable ``bytearray`` and ``memoryview`` buffers into immutable
    ``bytes`` instances, snapshots ``VectorView`` / ``VectorBatchView`` views into
    owned ``PgVector`` / ``PgArray`` records, and normalizes nested elements inside
    ``PgArray``, ``PgComposite``, ``PgJson``, ``PgParameter``, lists, tuples,
    and dicts without mutating caller containers.
    """
    if type(value) in _PRIMITIVE_PARAM_TYPES:
        return value
    if isinstance(value, PgUnchangedToast):
        raise TypeError("PG_UNCHANGED_TOAST cannot be bound as a SQL parameter")
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, VectorView):
        return PgVector(tuple(float(x) for x in value.to_list()), kind="vector")
    if isinstance(value, VectorBatchView):
        return tuple(
            PgVector(tuple(float(x) for x in value[idx].to_list()), kind="vector") for idx in range(len(value))
        )
    if isinstance(value, PgJson):
        return PgJson(_copy_json_container(value.value))
    if isinstance(value, PgParameter):
        inner = value.value
        if value.type_oid == PG_JSONB_TEXT and isinstance(inner, (dict, list)):
            return PgParameter(encode_json(inner), PG_JSONB_TEXT)
        return PgParameter(normalize_postgres_parameter(inner), value.type_oid)
    if isinstance(value, PgArray):
        normalized_vals = tuple(normalize_postgres_parameter(item) for item in value.values)
        return PgArray(
            element_oid=value.element_oid,
            dimensions=value.dimensions,
            lower_bounds=value.lower_bounds,
            values=normalized_vals,
        )
    if isinstance(value, PgComposite):
        normalized_fields = tuple(normalize_postgres_parameter(item) for item in value.values)
        return PgComposite(
            type_oid=value.type_oid,
            field_names=value.field_names,
            field_oids=value.field_oids,
            values=normalized_fields,
        )
    if isinstance(value, list):
        return [normalize_postgres_parameter(item) for item in value]
    if isinstance(value, tuple):
        return tuple(normalize_postgres_parameter(item) for item in value)
    if isinstance(value, dict):
        return {k: normalize_postgres_parameter(v) for k, v in value.items()}
    if isinstance(value, Buffer) and not isinstance(value, (str, bytes)):
        return bytes(memoryview(value))
    return value


def normalize_postgres_parameters(parameters: Sequence[Any] | None) -> tuple[Any, ...]:
    """Normalize a positional PostgreSQL parameter tuple into immutable owned values."""
    if not parameters:
        return ()
    if all(type(item) in _PRIMITIVE_PARAM_TYPES for item in parameters):
        return parameters if type(parameters) is tuple else tuple(parameters)
    return tuple(normalize_postgres_parameter(item) for item in parameters)


def compile_postgres_binds(
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
) -> CompiledPostgresBinds:
    """Compile SQL placeholders (`%s`, `%(name)s`, or `$N`) and normalize parameters.

    Delegates placeholder scanning and parameter ordering to ``vertebra._postgres.compile_binds``
    and normalizes the resulting parameter tuple into owned immutable values.
    """
    if not isinstance(sql, str):
        raise ProgrammingError("SQL query must be a str")

    compiled_sql, ordered_params = _resolve_native_compile_binds()(sql, parameters)
    normalized_params = normalize_postgres_parameters(ordered_params)

    if not normalized_params:
        style = "none"
    elif isinstance(parameters, Mapping):
        style = "named"
    elif compiled_sql == sql:
        style = "numeric"
    else:
        style = "pyformat"

    return CompiledPostgresBinds(sql=compiled_sql, params=normalized_params, style=style)


def _copy_json_container(value: Any) -> Any:
    """Deep-copy mutable dict/list containers inside JSON trees so caller mutations cannot alias."""
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    return value
