"""Single-pass PostgreSQL lexical bind compiler and immutable parameter normalizer."""

from __future__ import annotations

import copy
from collections.abc import Buffer, Mapping, Sequence
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
from vertebra.utils.serializers import encode_json

__all__ = (
    "PG_JSONB_TEXT",
    "CompiledPostgresBinds",
    "compile_postgres_binds",
    "normalize_postgres_parameter",
    "normalize_postgres_parameters",
)

PG_JSONB_TEXT: Final[int] = 3802


class CompiledPostgresBinds(msgspec.Struct, frozen=True):
    """Result of single-pass PostgreSQL bind compilation and parameter normalization."""

    sql: str
    params: tuple[Any, ...]
    style: str


def normalize_postgres_parameter(value: Any) -> Any:
    """Convert mutable/FFI parameter wrappers into immutable owned PostgreSQL values.

    Copies mutable ``bytearray`` and ``memoryview`` buffers into immutable
    ``bytes`` instances, snapshots ``VectorView`` / ``VectorBatchView`` views into
    owned ``PgVector`` / ``PgArray`` records, and normalizes nested elements inside
    ``PgArray``, ``PgComposite``, ``PgJson``, ``PgParameter``, lists, tuples,
    and dicts without mutating caller containers.
    """
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
    return tuple(normalize_postgres_parameter(item) for item in parameters)


def compile_postgres_binds(
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
) -> CompiledPostgresBinds:
    """Compile SQL placeholders (`%s`, `%(name)s`, or `$N`) and normalize parameters in one pass.

    Preserves numeric ``$N`` queries and parameterless SQL byte-for-byte without
    altering literals or comments, rejects mixed placeholder styles or ambiguous
    ``\\'`` escapes in ordinary single-quoted literals when pyformat conversion is
    active, deduplicates named ``%(name)s`` binds to shared ``$N`` slots, and
    returns owned immutable parameter values.
    """
    if not isinstance(sql, str):
        raise ProgrammingError("SQL query must be a str")

    if parameters is not None and isinstance(parameters, (str, bytes, bytearray, memoryview)):
        raise ProgrammingError("Query parameters must be a Sequence, Mapping, or None, not str or bytes")

    is_mapping = isinstance(parameters, Mapping)
    if parameters is not None and not is_mapping and not isinstance(parameters, Sequence):
        raise ProgrammingError(f"Unsupported parameters container type: {type(parameters).__name__}")

    n = len(sql)
    i = 0
    last_emit = 0
    state = _ScanState()

    while i < n:
        ch = sql[i]
        if ch in ("E", "e") and i + 1 < n and sql[i + 1] == "'" and (i == 0 or not _is_ident_char(sql[i - 1])):
            i = _scan_single_quoted(sql, i, state, c_escape=True)
            continue
        if ch == "'":
            i = _scan_single_quoted(sql, i, state, c_escape=False)
            continue
        if ch == '"':
            i = _scan_double_quoted(sql, i)
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            newline_idx = sql.find("\n", i + 2)
            i = n if newline_idx == -1 else newline_idx + 1
            continue
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            i = _scan_block_comment(sql, i)
            continue
        if ch == "$":
            preceded_by_ident = i > 0 and _is_ident_char(sql[i - 1])
            if not preceded_by_ident:
                tag_end = _match_dollar_quote(sql, i)
                if tag_end is not None:
                    tag = sql[i:tag_end]
                    close_idx = sql.find(tag, tag_end)
                    i = close_idx + len(tag) if close_idx != -1 else n
                    continue
                if i + 1 < n and sql[i + 1].isdigit():
                    j = i + 1
                    while j < n and sql[j].isdigit():
                        j += 1
                    if j == n or not _is_ident_char(sql[j]):
                        idx_val = int(sql[i + 1 : j])
                        state.record_numeric(idx_val)
                    i = j
                    continue
            i += 1
            continue
        if ch == "%":
            if i + 1 < n and sql[i + 1] == "%":
                out = _activate_rewrite(state, sql, last_emit)
                _emit_chunk(state, sql, last_emit, i)
                out.append("%")
                state.has_escaped_percent = True
                i += 2
                last_emit = i
                continue
            if i + 1 < n and sql[i + 1] == "s":
                if state.named_order is not None or state.numeric_indices is not None:
                    raise ProgrammingError(
                        "Cannot mix positional `%s` placeholders with named `%(name)s` or numeric `$N` placeholders"
                    )
                state.positional_count += 1
                out = _activate_rewrite(state, sql, last_emit)
                _emit_chunk(state, sql, last_emit, i)
                out.append(f"${state.positional_count}")
                i += 2
                last_emit = i
                continue
            if i + 1 < n and sql[i + 1] == "(":
                if state.positional_count > 0 or state.numeric_indices is not None:
                    raise ProgrammingError(
                        "Cannot mix named `%(name)s` placeholders with positional `%s` or numeric `$N` placeholders"
                    )
                close_paren = sql.find(")s", i + 2)
                if close_paren == -1:
                    raise ProgrammingError("Malformed named `%(name)s` placeholder in SQL")
                name = sql[i + 2 : close_paren]
                if not name:
                    raise ProgrammingError("Empty named `%()s` placeholder in SQL")
                named_order, named_slots = state.ensure_named()
                slot = named_slots.get(name)
                if slot is None:
                    slot = len(named_order) + 1
                    named_slots[name] = slot
                    named_order.append(name)
                out = _activate_rewrite(state, sql, last_emit)
                _emit_chunk(state, sql, last_emit, i)
                out.append(f"${slot}")
                i = close_paren + 2
                last_emit = i
                continue
        i += 1

    has_pyformat = state.positional_count > 0 or state.named_order is not None
    if has_pyformat and state.has_ambiguous_backslash_quote:
        raise ProgrammingError(
            "Ambiguous `\\'` escape inside ordinary single-quoted literal with pyformat placeholders; use `''` or `E'...'`"
        )

    if state.numeric_indices is not None:
        if isinstance(parameters, Mapping):
            raise ProgrammingError(
                "Numeric `$N` SQL placeholders require a positional Sequence of parameters, not a Mapping"
            )
        if state.numeric_indices and any(idx < 1 for idx in state.numeric_indices):
            raise ProgrammingError("PostgreSQL numeric bind placeholders must start at `$1`")
        seq_params = normalize_postgres_parameters(parameters)
        max_idx = max(state.numeric_indices)
        if len(seq_params) < max_idx:
            raise ProgrammingError(
                f"SQL references `${max_idx}` but only {len(seq_params)} positional parameters were supplied"
            )
        return CompiledPostgresBinds(sql=sql, params=seq_params, style="numeric")

    if state.named_order is not None:
        if not isinstance(parameters, Mapping):
            raise ProgrammingError("Named `%(name)s` SQL placeholders require a Mapping of parameters")
        assert state.out is not None
        _emit_chunk(state, sql, last_emit, n)
        compiled_sql = "".join(state.out)
        bound: list[Any] = []
        for name in state.named_order:
            if name not in parameters:
                raise ProgrammingError(f"Missing required named SQL parameter: {name!r}")
            bound.append(normalize_postgres_parameter(parameters[name]))
        return CompiledPostgresBinds(sql=compiled_sql, params=tuple(bound), style="named")

    if state.positional_count > 0:
        if isinstance(parameters, Mapping):
            raise ProgrammingError("Positional `%s` SQL placeholders require a Sequence of parameters, not a Mapping")
        seq_params = normalize_postgres_parameters(parameters)
        if len(seq_params) != state.positional_count:
            raise ProgrammingError(
                f"SQL has {state.positional_count} `%s` placeholders but {len(seq_params)} parameters were supplied"
            )
        assert state.out is not None
        _emit_chunk(state, sql, last_emit, n)
        compiled_sql = "".join(state.out)
        return CompiledPostgresBinds(sql=compiled_sql, params=seq_params, style="pyformat")

    if parameters is None or len(parameters) == 0:
        return CompiledPostgresBinds(sql=sql, params=(), style="none")

    if is_mapping:
        raise ProgrammingError(
            "Mapping parameters were supplied for a SQL statement without named `%(name)s` placeholders"
        )
    raise ProgrammingError(
        f"Positional parameters ({len(parameters)}) were supplied for a SQL statement with 0 placeholders"
    )


class _ScanState(msgspec.Struct):
    """Mutable single-pass lexical scanner accumulator."""

    positional_count: int = 0
    named_order: list[str] | None = None
    named_slots: dict[str, int] | None = None
    numeric_indices: list[int] | None = None
    has_escaped_percent: bool = False
    has_ambiguous_backslash_quote: bool = False
    out: list[str] | None = None

    def ensure_named(self) -> tuple[list[str], dict[str, int]]:
        """Lazily initialize named placeholder tracking structures."""
        if self.named_order is None:
            self.named_order = []
        if self.named_slots is None:
            self.named_slots = {}
        return self.named_order, self.named_slots

    def record_numeric(self, index: int) -> None:
        """Record an observed native numeric ``$N`` bind index, rejecting mixed styles."""
        if self.positional_count > 0 or self.named_order is not None:
            raise ProgrammingError(
                "Cannot mix numeric `$N` placeholders with positional `%s` or named `%(name)s` placeholders"
            )
        if self.numeric_indices is None:
            self.numeric_indices = []
        self.numeric_indices.append(index)


def _is_ident_char(ch: str) -> bool:
    """Return whether ``ch`` can be part of an unquoted SQL identifier."""
    return ch.isalnum() or ch == "_" or ch == "$"


def _is_dollar_tag_start(ch: str) -> bool:
    """Return whether ``ch`` is valid as the first character of a ``$tag$`` delimiter."""
    return ch.isalpha() or ch == "_"


def _is_dollar_tag_char(ch: str) -> bool:
    """Return whether ``ch`` is valid inside a ``$tag$`` delimiter."""
    return ch.isalnum() or ch == "_"


def _copy_json_container(value: Any) -> Any:
    """Deep-copy mutable dict/list containers inside JSON trees so caller mutations cannot alias."""
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    return value


def _match_dollar_quote(sql: str, pos: int) -> int | None:
    """Return end index after closing ``$`` of a ``$tag$`` opener, or ``None``."""
    n = len(sql)
    if pos + 1 >= n:
        return None
    if sql[pos + 1] == "$":
        return pos + 2
    if not _is_dollar_tag_start(sql[pos + 1]):
        return None
    j = pos + 2
    while j < n and _is_dollar_tag_char(sql[j]):
        j += 1
    if j < n and sql[j] == "$":
        return j + 1
    return None


def _scan_single_quoted(sql: str, pos: int, state: _ScanState, *, c_escape: bool) -> int:
    """Advance past an ordinary or ``E'...'`` single-quoted SQL string literal."""
    n = len(sql)
    i = pos + (2 if c_escape else 1)
    trailing_backslashes = 0
    while i < n:
        ch = sql[i]
        if c_escape:
            if ch == "\\":
                i += 2
                continue
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    i += 2
                    continue
                return i + 1
            i += 1
            continue
        if ch == "\\":
            trailing_backslashes += 1
            i += 1
            continue
        if ch == "'":
            if trailing_backslashes % 2 == 1:
                state.has_ambiguous_backslash_quote = True
            trailing_backslashes = 0
            if i + 1 < n and sql[i + 1] == "'":
                i += 2
                continue
            return i + 1
        trailing_backslashes = 0
        i += 1
    return n


def _scan_double_quoted(sql: str, pos: int) -> int:
    """Advance past a ``"..."`` double-quoted SQL identifier."""
    n = len(sql)
    i = pos + 1
    while i < n:
        if sql[i] == '"':
            if i + 1 < n and sql[i + 1] == '"':
                i += 2
                continue
            return i + 1
        i += 1
    return n


def _scan_block_comment(sql: str, pos: int) -> int:
    """Advance past nested ``/* ... */`` PostgreSQL block comments."""
    n = len(sql)
    depth = 1
    i = pos + 2
    while i < n and depth > 0:
        if sql[i] == "/" and i + 1 < n and sql[i + 1] == "*":
            depth += 1
            i += 2
        elif sql[i] == "*" and i + 1 < n and sql[i + 1] == "/":
            depth -= 1
            i += 2
        else:
            i += 1
    return i


def _emit_chunk(state: _ScanState, sql: str, start: int, end: int) -> None:
    """Append slice ``sql[start:end]`` when rewriting pyformat output."""
    if state.out is not None and end > start:
        state.out.append(sql[start:end])


def _activate_rewrite(state: _ScanState, sql: str, copy_until: int) -> list[str]:
    """Lazily allocate output chunk buffer preserving untouched prefix."""
    if state.out is None:
        state.out = [sql[:copy_until]] if copy_until > 0 else []
    return state.out
