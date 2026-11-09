"""Owned PostgreSQL value records, query results, and pipeline outcome models."""

from __future__ import annotations

import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Literal
from uuid import UUID

import msgspec

__all__ = [
    "PgArray",
    "PgBitString",
    "PgComposite",
    "PgInterval",
    "PgJson",
    "PgParameter",
    "PgTemporal",
    "PgVector",
    "PostgresColumn",
    "PostgresErrorInfo",
    "PostgresJsonValue",
    "PostgresPipelineOperation",
    "PostgresPipelineOutcome",
    "PostgresQueryResult",
    "PostgresValue",
    "TemporalKind",
    "TemporalSpecial",
    "_project_public_row",
]

type TemporalKind = Literal["date", "time", "timestamp", "timestamptz"]
type TemporalSpecial = Literal[
    "positive_infinity",
    "negative_infinity",
    "out_of_range",
    "end_of_day",
    "wire_text",
]
type PostgresJsonValue = None | bool | int | float | str | list[PostgresJsonValue] | dict[str, PostgresJsonValue]

_I16_MIN = -(1 << 15)
_I16_MAX = (1 << 15) - 1
_I32_MIN = -(1 << 31)
_I32_MAX = (1 << 31) - 1
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_U32_MAX = (1 << 32) - 1
_U64_MAX = (1 << 64) - 1

_PG_DATE_MIN_FINITE = -2_451_545
_PG_DATE_MAX_FINITE = 2_147_483_493
_PY_DATE_MIN_PG_DAYS = -730_119
_PY_DATE_MAX_PG_DAYS = 2_921_939

_PG_TIME_MAX_MICROS = 86_400_000_000

_PG_TS_MIN_FINITE = -211_813_488_000_000_000
_PG_TS_MAX_FINITE = 9_223_371_331_200_000_000
_PY_TS_MIN_PG_MICROS = -63_113_904_000_000_000
_PY_TS_MAX_PG_MICROS = 252_455_616_000_000_000

_F32_MAX = 3.4028234663852886e38
_F16_MAX = 65504.0


class PostgresColumn(msgspec.Struct, frozen=True, gc=False):
    """Owned PostgreSQL result column descriptor."""

    name: str
    type_oid: int
    format: Literal[0, 1]
    type_modifier: int
    table_oid: int
    attribute_number: int

    def __post_init__(self) -> None:
        """Validate column descriptor types and integer bounds."""
        if not isinstance(self.name, str):
            msg = "PostgresColumn.name must be a str"
            raise TypeError(msg)
        _validate_int(self.type_oid, "type_oid", 1, _U32_MAX)
        _validate_int(self.format, "format", 0, 1)
        _validate_int(self.type_modifier, "type_modifier", _I32_MIN, _I32_MAX)
        _validate_int(self.table_oid, "table_oid", 0, _U32_MAX)
        _validate_int(self.attribute_number, "attribute_number", _I16_MIN, _I16_MAX)


class PostgresQueryResult(msgspec.Struct, frozen=True, gc=False):
    """Owned PostgreSQL single-statement or simple-query result payload."""

    columns: tuple[PostgresColumn, ...]
    rows: tuple[tuple[PostgresValue, ...], ...]
    affected_rows: int
    command_tag: str
    transaction_status: Literal["I", "T", "E"]

    def __post_init__(self) -> None:
        """Validate column/row tuple types, row width parity, and command metadata."""
        if not isinstance(self.columns, tuple) or not all(isinstance(col, PostgresColumn) for col in self.columns):
            msg = "PostgresQueryResult.columns must be a tuple of PostgresColumn"
            raise TypeError(msg)
        if not isinstance(self.rows, tuple):
            msg = "PostgresQueryResult.rows must be a tuple of row tuples"
            raise TypeError(msg)
        expected_width = len(self.columns)
        for row in self.rows:
            if not isinstance(row, tuple):
                msg = "Each PostgresQueryResult row must be a tuple"
                raise TypeError(msg)
            if len(row) != expected_width:
                msg = f"Row width {len(row)} does not match column count {expected_width}"
                raise ValueError(msg)
            for cell in row:
                _reject_nested_parameter(cell)
        _validate_int(self.affected_rows, "affected_rows", 0, _U64_MAX)
        if not isinstance(self.command_tag, str):
            msg = "PostgresQueryResult.command_tag must be a str"
            raise TypeError(msg)
        if self.transaction_status not in ("I", "T", "E"):
            msg = "PostgresQueryResult.transaction_status must be 'I', 'T', or 'E'"
            raise ValueError(msg)


class PgInterval(msgspec.Struct, frozen=True, gc=False):
    """Lossless PostgreSQL interval preserving independent signed months, days, and microseconds."""

    months: int
    days: int
    microseconds: int

    def __post_init__(self) -> None:
        """Validate signed 32-bit month/day and signed 64-bit microsecond bounds."""
        _validate_int(self.months, "months", _I32_MIN, _I32_MAX)
        _validate_int(self.days, "days", _I32_MIN, _I32_MAX)
        _validate_int(self.microseconds, "microseconds", _I64_MIN, _I64_MAX)


class PgArray(msgspec.Struct, frozen=True, gc=False):
    """Lossless PostgreSQL multidimensional array with explicit lower bounds and element OID."""

    values: tuple[PostgresValue, ...]
    dimensions: tuple[int, ...]
    lower_bounds: tuple[int, ...]
    element_oid: int

    def __post_init__(self) -> None:
        """Validate array rank, dimension bounds, lower bounds, and flat element count."""
        if not isinstance(self.values, tuple):
            msg = "PgArray.values must be a tuple"
            raise TypeError(msg)
        if not isinstance(self.dimensions, tuple) or not isinstance(self.lower_bounds, tuple):
            msg = "PgArray.dimensions and lower_bounds must be tuples"
            raise TypeError(msg)
        _validate_int(self.element_oid, "element_oid", 1, _U32_MAX)
        rank = len(self.dimensions)
        if rank > 6:
            msg = f"PgArray rank {rank} exceeds PostgreSQL maximum rank 6"
            raise ValueError(msg)
        if len(self.lower_bounds) != rank:
            msg = "PgArray.dimensions and lower_bounds must have equal length"
            raise ValueError(msg)
        if rank == 0:
            if len(self.values) != 0:
                msg = "Rank-0 PgArray must have empty values"
                raise ValueError(msg)
            return
        product = 1
        for dim, lbound in zip(self.dimensions, self.lower_bounds, strict=True):
            _validate_int(dim, "dimension", 1, _I32_MAX)
            _validate_int(lbound, "lower_bound", _I32_MIN, _I32_MAX)
            if lbound + dim - 1 > _I32_MAX:
                msg = "PgArray upper bound overflows signed 32-bit integer"
                raise ValueError(msg)
            product *= dim
            if product > _I32_MAX:
                msg = "PgArray element count overflows signed 32-bit integer"
                raise ValueError(msg)
        if product != len(self.values):
            msg = f"PgArray dimension product {product} does not match values length {len(self.values)}"
            raise ValueError(msg)
        for item in self.values:
            _reject_nested_parameter(item)


class PgComposite(msgspec.Struct, frozen=True, gc=False):
    """Lossless PostgreSQL composite or record value with ordered field names and OIDs."""

    type_oid: int
    field_names: tuple[str, ...]
    field_oids: tuple[int, ...]
    values: tuple[PostgresValue, ...]

    def __post_init__(self) -> None:
        """Validate composite OID, field names, field OIDs, and value parity."""
        _validate_int(self.type_oid, "type_oid", 1, _U32_MAX)
        if (
            not isinstance(self.field_names, tuple)
            or not isinstance(self.field_oids, tuple)
            or not isinstance(self.values, tuple)
        ):
            msg = "PgComposite field_names, field_oids, and values must be tuples"
            raise TypeError(msg)
        if not (len(self.field_names) == len(self.field_oids) == len(self.values)):
            msg = "PgComposite field_names, field_oids, and values must have equal length"
            raise ValueError(msg)
        for name in self.field_names:
            if not isinstance(name, str):
                msg = "PgComposite field_names must contain str values"
                raise TypeError(msg)
        for oid in self.field_oids:
            _validate_int(oid, "field_oid", 1, _U32_MAX)
        for item in self.values:
            _reject_nested_parameter(item)


class PgTemporal(msgspec.Struct, frozen=True, gc=False):
    """Lossless PostgreSQL temporal wrapper for infinities, 24:00, out-of-range dates, and wire_text."""

    kind: TemporalKind
    value: int | None
    special: TemporalSpecial | None
    wire_text: str | None = None
    date_style: str | None = None
    time_zone: str | None = None

    def __post_init__(self) -> None:
        """Validate temporal kind, sentinel invariants, and wire_text exclusivity."""
        if self.kind not in ("date", "time", "timestamp", "timestamptz"):
            msg = f"Invalid PgTemporal.kind: {self.kind!r}"
            raise ValueError(msg)
        if self.special not in (
            None,
            "positive_infinity",
            "negative_infinity",
            "out_of_range",
            "end_of_day",
            "wire_text",
        ):
            msg = f"Invalid PgTemporal.special: {self.special!r}"
            raise ValueError(msg)

        if self.special == "wire_text":
            if self.kind != "timestamptz" or self.value is not None:
                msg = "PgTemporal wire_text requires kind='timestamptz' and value=None"
                raise ValueError(msg)
            if (
                not isinstance(self.wire_text, str)
                or not self.wire_text
                or not isinstance(self.date_style, str)
                or not self.date_style
                or not isinstance(self.time_zone, str)
                or not self.time_zone
            ):
                msg = "PgTemporal wire_text requires non-empty wire_text, date_style, and time_zone"
                raise ValueError(msg)
            return

        if self.wire_text is not None or self.date_style is not None or self.time_zone is not None:
            msg = "Raw PgTemporal values must leave wire_text, date_style, and time_zone as None"
            raise ValueError(msg)
        if self.value is None:
            msg = "Raw PgTemporal values require an integer value"
            raise ValueError(msg)

        if self.kind == "date":
            _validate_int(self.value, "value", _I32_MIN, _I32_MAX)
            if self.special == "positive_infinity":
                if self.value != _I32_MAX:
                    msg = "date positive_infinity requires value == 2**31 - 1"
                    raise ValueError(msg)
            elif self.special == "negative_infinity":
                if self.value != _I32_MIN:
                    msg = "date negative_infinity requires value == -2**31"
                    raise ValueError(msg)
            elif self.special == "end_of_day":
                msg = "date does not support end_of_day"
                raise ValueError(msg)
            else:
                if not (_PG_DATE_MIN_FINITE <= self.value <= _PG_DATE_MAX_FINITE):
                    msg = "Finite PostgreSQL date value is outside server calendar range"
                    raise ValueError(msg)
                if self.special == "out_of_range" and (_PY_DATE_MIN_PG_DAYS <= self.value <= _PY_DATE_MAX_PG_DAYS):
                    msg = "date value inside Python date range cannot use special='out_of_range'"
                    raise ValueError(msg)
            return

        if self.kind == "time":
            if self.special not in (None, "end_of_day"):
                msg = "time only supports special=None or special='end_of_day'"
                raise ValueError(msg)
            _validate_int(self.value, "value", 0, _PG_TIME_MAX_MICROS)
            if self.special == "end_of_day" and self.value != _PG_TIME_MAX_MICROS:
                msg = "time end_of_day requires value == 86_400_000_000"
                raise ValueError(msg)
            return

        _validate_int(self.value, "value", _I64_MIN, _I64_MAX)
        if self.special == "positive_infinity":
            if self.value != _I64_MAX:
                msg = f"{self.kind} positive_infinity requires value == 2**63 - 1"
                raise ValueError(msg)
        elif self.special == "negative_infinity":
            if self.value != _I64_MIN:
                msg = f"{self.kind} negative_infinity requires value == -2**63"
                raise ValueError(msg)
        elif self.special == "end_of_day":
            msg = f"{self.kind} does not support end_of_day"
            raise ValueError(msg)
        else:
            if not (_PG_TS_MIN_FINITE <= self.value <= _PG_TS_MAX_FINITE):
                msg = f"Finite PostgreSQL {self.kind} value is outside server range"
                raise ValueError(msg)
            if self.special == "out_of_range" and (_PY_TS_MIN_PG_MICROS <= self.value < _PY_TS_MAX_PG_MICROS):
                msg = f"{self.kind} value inside Python datetime range cannot use special='out_of_range'"
                raise ValueError(msg)


class PgJson(msgspec.Struct, frozen=True, gc=False):
    """Explicit PostgreSQL JSON/JSONB wrapper distinguishing JSON null from SQL NULL."""

    value: PostgresJsonValue

    def __post_init__(self) -> None:
        """Validate JSON value tree for supported types, finite floats, and acyclicity."""
        _validate_json_tree(self.value)


class PgBitString(msgspec.Struct, frozen=True, gc=False):
    """Lossless PostgreSQL BIT/VARBIT bitstring preserving MSB-first bits and exact bit length."""

    data: bytes
    bit_length: int

    def __post_init__(self) -> None:
        """Validate immutable bytes storage, byte length parity, and zero low-bit padding."""
        if type(self.data) is not bytes:
            msg = "PgBitString.data must be exact bytes"
            raise TypeError(msg)
        _validate_int(self.bit_length, "bit_length", 0, _I32_MAX)
        expected_len = (self.bit_length + 7) // 8
        if len(self.data) != expected_len:
            msg = f"PgBitString byte length {len(self.data)} does not match ceil({self.bit_length}/8) == {expected_len}"
            raise ValueError(msg)
        rem = self.bit_length % 8
        if rem != 0:
            mask = (1 << (8 - rem)) - 1
            if (self.data[-1] & mask) != 0:
                msg = "PgBitString has non-zero unused low padding bits in trailing byte"
                raise ValueError(msg)


class PgVector(msgspec.Struct, frozen=True, gc=False):
    """Lossless pgvector vector / halfvec record with 1..=16000 finite dimensions."""

    values: tuple[float, ...]
    kind: Literal["vector", "halfvec"]

    def __post_init__(self) -> None:
        """Validate vector kind, dimension count 1..=16000, and finite float elements."""
        if self.kind not in ("vector", "halfvec"):
            msg = "PgVector.kind must be 'vector' or 'halfvec'"
            raise ValueError(msg)
        if not isinstance(self.values, tuple):
            msg = "PgVector.values must be a tuple"
            raise TypeError(msg)
        dim = len(self.values)
        if dim < 1 or dim > 16000:
            msg = f"PgVector dimension {dim} must be between 1 and 16000 inclusive"
            raise ValueError(msg)
        max_abs = _F16_MAX if self.kind == "halfvec" else _F32_MAX
        need_float_copy = False
        for item in self.values:
            if isinstance(item, bool) or not isinstance(item, (int, float)):
                msg = "PgVector elements must be finite int or float values (not bool)"
                raise TypeError(msg)
            fval = float(item)
            if not math.isfinite(fval) or abs(fval) > max_abs:
                msg = f"PgVector element {item!r} is not finite within {self.kind} range"
                raise ValueError(msg)
            if type(item) is not float:
                need_float_copy = True
        if need_float_copy:
            msgspec.structs.force_setattr(self, "values", tuple(float(x) for x in self.values))


class PgParameter(msgspec.Struct, frozen=True, gc=False):
    """Explicit outer query parameter binding value with target PostgreSQL type OID."""

    value: PostgresValue
    type_oid: int

    def __post_init__(self) -> None:
        """Validate positive 32-bit target OID and forbid nested PgParameter wrappers."""
        _validate_int(self.type_oid, "type_oid", 1, _U32_MAX)
        _reject_nested_parameter(self.value)


class PostgresErrorInfo(msgspec.Struct, frozen=True, gc=False):
    """Owned PostgreSQL server error diagnostic record for pipeline outcomes."""

    sqlstate: str | None
    message: str
    severity: str | None
    diagnostics: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        """Validate SQLSTATE, message, severity, and diagnostic key-value pairs."""
        if self.sqlstate is not None:
            if not isinstance(self.sqlstate, str):
                msg = "PostgresErrorInfo.sqlstate must be str or None"
                raise TypeError(msg)
            if len(self.sqlstate) != 5 or not self.sqlstate.isascii():
                msg = "PostgresErrorInfo.sqlstate must be a 5-character ASCII string"
                raise ValueError(msg)
        if not isinstance(self.message, str):
            msg = "PostgresErrorInfo.message must be a str"
            raise TypeError(msg)
        if self.severity is not None and not isinstance(self.severity, str):
            msg = "PostgresErrorInfo.severity must be str or None"
            raise TypeError(msg)
        if not isinstance(self.diagnostics, tuple):
            msg = "PostgresErrorInfo.diagnostics must be a tuple of (str, str) pairs"
            raise TypeError(msg)
        for pair in self.diagnostics:
            if (
                not isinstance(pair, tuple)
                or len(pair) != 2
                or not isinstance(pair[0], str)
                or not isinstance(pair[1], str)
            ):
                msg = "Each diagnostic entry must be a (str, str) tuple"
                raise TypeError(msg)


class PostgresPipelineOperation(msgspec.Struct, frozen=True, gc=False):
    """Single extended-query operation inside a single-Sync PostgreSQL pipeline batch."""

    sql: str
    parameters: tuple[PostgresValue | PgParameter, ...] = ()
    parameter_oids: tuple[int, ...] | None = None
    kind: Literal["execute", "select"] = "execute"

    def __post_init__(self) -> None:
        """Validate SQL text, parameters tuple, parameter_oids parity, and projection kind."""
        if not isinstance(self.sql, str):
            msg = "PostgresPipelineOperation.sql must be a str"
            raise TypeError(msg)
        if not isinstance(self.parameters, tuple):
            msg = "PostgresPipelineOperation.parameters must be a tuple"
            raise TypeError(msg)
        if self.kind not in ("execute", "select"):
            msg = "PostgresPipelineOperation.kind must be 'execute' or 'select'"
            raise ValueError(msg)
        for param in self.parameters:
            if isinstance(param, PgParameter):
                _reject_nested_parameter(param.value)
            else:
                _reject_nested_parameter(param)
        if self.parameter_oids is not None:
            if not isinstance(self.parameter_oids, tuple):
                msg = "PostgresPipelineOperation.parameter_oids must be a tuple or None"
                raise TypeError(msg)
            if len(self.parameter_oids) != len(self.parameters):
                msg = "parameter_oids length must match parameters length"
                raise ValueError(msg)
            for oid, param in zip(self.parameter_oids, self.parameters, strict=True):
                _validate_int(oid, "parameter_oid", 0, _U32_MAX)
                if isinstance(param, PgParameter) and oid != 0 and oid != param.type_oid:
                    msg = f"Conflicting parameter_oids entry {oid} and PgParameter.type_oid {param.type_oid}"
                    raise ValueError(msg)


class PostgresPipelineOutcome(msgspec.Struct, frozen=True, gc=False):
    """Ordered per-operation outcome from a single-Sync PostgreSQL pipeline batch."""

    index: int
    status: Literal["result", "error", "skipped"]
    result: PostgresQueryResult | None = None
    error: PostgresErrorInfo | None = None
    failed_index: int | None = None

    def __post_init__(self) -> None:
        """Validate outcome status discriminant and mutually exclusive payload fields."""
        _validate_int(self.index, "index", 0, _I32_MAX)
        if self.status == "result":
            if not isinstance(self.result, PostgresQueryResult):
                msg = "status='result' requires a PostgresQueryResult"
                raise ValueError(msg)
            if self.error is not None or self.failed_index is not None:
                msg = "status='result' must have error=None and failed_index=None"
                raise ValueError(msg)
        elif self.status == "error":
            if not isinstance(self.error, PostgresErrorInfo):
                msg = "status='error' requires a PostgresErrorInfo"
                raise ValueError(msg)
            if self.result is not None or self.failed_index is not None:
                msg = "status='error' must have result=None and failed_index=None"
                raise ValueError(msg)
        elif self.status == "skipped":
            if self.failed_index is None:
                msg = "status='skipped' requires failed_index"
                raise ValueError(msg)
            _validate_int(self.failed_index, "failed_index", 0, _I32_MAX)
            if self.failed_index >= self.index:
                msg = "failed_index must reference an earlier operation index"
                raise ValueError(msg)
            if self.result is not None or self.error is not None:
                msg = "status='skipped' must have result=None and error=None"
                raise ValueError(msg)
        else:
            msg = f"Invalid PostgresPipelineOutcome.status: {self.status!r}"
            raise ValueError(msg)


type PostgresValue = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | Decimal
    | UUID
    | date
    | time
    | datetime
    | PgInterval
    | PgArray
    | PgComposite
    | PgTemporal
    | PgJson
    | PgBitString
    | PgVector
    | list[PostgresValue]
    | tuple[PostgresValue, ...]
    | dict[str, PostgresJsonValue]
)


def _project_public_row(row: tuple[PostgresValue, ...]) -> tuple[PostgresValue, ...]:
    """Map only top-level PgJson(None) cells to None for high-level public driver rows."""
    if not any(isinstance(cell, PgJson) and cell.value is None for cell in row):
        return row
    return tuple(None if isinstance(cell, PgJson) and cell.value is None else cell for cell in row)


def _validate_int(value: object, field_name: str, min_value: int, max_value: int) -> None:
    """Validate that value is an exact int (not bool) within [min_value, max_value]."""
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{field_name} must be an int (not bool)"
        raise TypeError(msg)
    if value < min_value or value > max_value:
        msg = f"{field_name}={value} is outside allowed range [{min_value}, {max_value}]"
        raise ValueError(msg)


def _reject_nested_parameter(value: object) -> None:
    """Ensure PgParameter never appears nested inside a SQL value or container."""
    stack: list[object] = [value]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, PgParameter):
            msg = "PgParameter cannot be nested inside another wrapper or container"
            raise ValueError(msg)
        if isinstance(current, (PgArray, PgComposite)):
            stack.extend(current.values)
        elif isinstance(current, PgJson):
            stack.append(current.value)
        elif isinstance(current, (list, tuple)):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            stack.extend(current)
        elif isinstance(current, dict):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            stack.extend(current.values())


def _validate_json_tree(root: object) -> None:
    """Validate that a PgJson value is a cycle-free RFC 8259 JSON tree with finite floats."""
    stack: list[tuple[object, frozenset[int]]] = [(root, frozenset())]
    while stack:
        node, ancestors = stack.pop()
        if node is None or isinstance(node, (bool, int, str)):
            continue
        if isinstance(node, float):
            if not math.isfinite(node):
                msg = "PgJson float values must be finite"
                raise ValueError(msg)
            continue
        if isinstance(node, list):
            node_id = id(node)
            if node_id in ancestors:
                msg = "PgJson does not allow cyclic containers"
                raise ValueError(msg)
            next_ancestors = ancestors | {node_id}
            for item in node:
                stack.append((item, next_ancestors))
            continue
        if isinstance(node, dict):
            node_id = id(node)
            if node_id in ancestors:
                msg = "PgJson does not allow cyclic containers"
                raise ValueError(msg)
            next_ancestors = ancestors | {node_id}
            for key, val in node.items():
                if not isinstance(key, str):
                    msg = "PgJson object keys must be str"
                    raise TypeError(msg)
                stack.append((val, next_ancestors))
            continue
        msg = f"Unsupported PgJson value type: {type(node).__name__}"
        raise TypeError(msg)
