"""PostgreSQL binary JSONB and PGCOPY wire codecs."""

from __future__ import annotations

import contextlib
import struct
from collections.abc import Buffer, Mapping, Sequence
from typing import Any

from vertebra.db.adapters.postgres.binds import (
    CompiledPostgresBinds,
    compile_postgres_binds,
    normalize_postgres_parameters,
)
from vertebra.db.rls import TenantPredicate, TenantScopeConfig
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "PGCOPY_BINARY_SIGNATURE",
    "PostgreSQLCore",
    "compile_pg_binds",
    "decode_pgcopy_rows",
    "encode_pg_jsonb",
    "encode_pgcopy_rows",
    "normalize_pg_parameters",
    "strip_pg_jsonb_header",
)

PGCOPY_BINARY_SIGNATURE: bytes = b"PGCOPY\n\xff\r\n\x00"


_DEFAULT_TENANT_SCOPE = TenantScopeConfig()


class PostgreSQLCore:
    """Protocol codecs and wire helpers for PostgreSQL binary JSONB and PGCOPY."""

    @staticmethod
    def build_session_guc_sql(
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
        *,
        is_local: bool = True,
    ) -> tuple[str, tuple[str, str, bool]]:
        """Build a bound PostgreSQL session setting statement."""
        return "SELECT set_config($1, $2, $3)", (config.setting_name, tenant_value, is_local)

    @staticmethod
    def build_tenant_predicate_sql(predicate: TenantPredicate, *, param_index: int = 1) -> str:
        """Render a PostgreSQL numeric bind with the optional cast on its value."""
        suffix = f"::{predicate.config.parameter_cast}" if predicate.config.parameter_cast else ""
        return f"{predicate.column_reference()} = ${param_index}{suffix}"

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            for fn_name in ("strip_pg_jsonb_header", "strip_pg_jsonb_header_native"):
                native_fn = getattr(native_mod, fn_name, None)
                if callable(native_fn):
                    res = native_fn(bytes(payload))
                    if isinstance(res, (bytes, bytearray, memoryview)):
                        return memoryview(res)

        view = memoryview(payload).cast("B")
        if len(view) == 0:
            msg = "Empty PostgreSQL binary JSONB payload"
            raise ValueError(msg)
        if view[0] != 0x01:
            msg = f"Unsupported PostgreSQL binary JSONB version byte: 0x{view[0]:02x}"
            raise ValueError(msg)
        return view[1:]

    @staticmethod
    def encode_jsonb(value: Any) -> bytes:
        """Encode a Python value or UTF-8 JSON buffer with the 1-byte PostgreSQL `\\x01` JSONB header."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            for fn_name in ("encode_pg_jsonb", "encode_pg_jsonb_native"):
                native_fn = getattr(native_mod, fn_name, None)
                if callable(native_fn):
                    with contextlib.suppress(TypeError, ValueError, RuntimeError):
                        raw_arg = (
                            value
                            if isinstance(value, (bytes, bytearray))
                            else (value.encode("utf-8") if isinstance(value, str) else encode_json_bytes(value))
                        )
                        res = native_fn(raw_arg)
                        if isinstance(res, (bytes, bytearray)):
                            return bytes(res)

        if isinstance(value, (bytes, bytearray, memoryview)):
            raw_bytes = bytes(value)
            if raw_bytes.startswith(b"\x01"):
                return raw_bytes
            return b"\x01" + raw_bytes
        if isinstance(value, str):
            return b"\x01" + value.encode("utf-8")
        return b"\x01" + encode_json_bytes(value)

    @staticmethod
    def encode_pgcopy_rows(
        rows: Sequence[Sequence[Any]],
        flags: int = 0,
        ext_bytes: bytes = b"",
    ) -> bytes:
        """Encode a sequence of rows into PostgreSQL binary COPY format."""
        normalized_rows: list[list[bytes | None]] = [
            [
                None
                if field is None
                else (
                    field.encode("utf-8")
                    if isinstance(field, str)
                    else (
                        bytes(field)
                        if isinstance(field, (bytes, bytearray, memoryview))
                        else str(field).encode("utf-8")
                    )
                )
                for field in row
            ]
            for row in rows
        ]
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            for fn_name in ("encode_pgcopy_rows", "encode_pgcopy_rows_native"):
                native_fn = getattr(native_mod, fn_name, None)
                if callable(native_fn) and flags == 0 and not ext_bytes:
                    with contextlib.suppress(TypeError, ValueError, RuntimeError):
                        res = native_fn(normalized_rows)
                        if isinstance(res, (bytes, bytearray)):
                            return bytes(res)

        buf = bytearray(PGCOPY_BINARY_SIGNATURE)
        buf.extend(struct.pack(">II", flags, len(ext_bytes)))
        buf.extend(ext_bytes)
        for row in normalized_rows:
            buf.extend(struct.pack(">h", len(row)))
            for field in row:
                if field is None:
                    buf.extend(struct.pack(">i", -1))
                else:
                    buf.extend(struct.pack(">i", len(field)))
                    buf.extend(field)
        buf.extend(struct.pack(">h", -1))
        return bytes(buf)

    @staticmethod
    def decode_pgcopy_rows(payload: Buffer) -> list[tuple[bytes | None, ...]]:
        """Decode a PostgreSQL binary COPY stream into row tuples of bytes or None."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            for fn_name in ("decode_pgcopy_rows", "parse_pgcopy_binary_native", "parse_pgcopy_binary_rows"):
                native_fn = getattr(native_mod, fn_name, None)
                if callable(native_fn):
                    with contextlib.suppress(TypeError, ValueError):
                        res = native_fn(bytes(payload))
                        return [tuple(None if f is None else bytes(f) for f in r) for r in res]

        raw = memoryview(payload)
        sig_len = len(PGCOPY_BINARY_SIGNATURE)
        if len(raw) < sig_len or bytes(raw[:sig_len]) != PGCOPY_BINARY_SIGNATURE:
            msg = "Invalid PGCOPY binary signature"
            raise ValueError(msg)

        if len(raw) < sig_len + 8:
            msg = "Truncated PGCOPY header"
            raise ValueError(msg)

        _flags, ext_len = struct.unpack_from(">II", raw, sig_len)
        pos = sig_len + 8 + ext_len
        if pos > len(raw):
            msg = "Corrupted PGCOPY header extension"
            raise ValueError(msg)

        rows: list[tuple[bytes | None, ...]] = []
        while pos + 2 <= len(raw):
            (field_count,) = struct.unpack_from(">h", raw, pos)
            pos += 2
            if field_count == -1:
                return rows
            if field_count < -1:
                msg = f"Invalid PGCOPY field count: {field_count}"
                raise ValueError(msg)

            fields: list[bytes | None] = []
            for _ in range(field_count):
                if pos + 4 > len(raw):
                    msg = "Truncated PGCOPY field length"
                    raise ValueError(msg)
                (field_len,) = struct.unpack_from(">i", raw, pos)
                pos += 4
                if field_len == -1:
                    fields.append(None)
                elif field_len >= 0:
                    if pos + field_len > len(raw):
                        msg = "Truncated PGCOPY field data"
                        raise ValueError(msg)
                    fields.append(bytes(raw[pos : pos + field_len]))
                    pos += field_len
                else:
                    msg = f"Invalid PGCOPY field length: {field_len}"
                    raise ValueError(msg)
            rows.append(tuple(fields))

        msg = "Unexpected EOF before PGCOPY trailer"
        raise ValueError(msg)

    @staticmethod
    def compile_binds(
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> CompiledPostgresBinds:
        """Compile PostgreSQL `%s`, `%(name)s`, or `$N` placeholders and normalize binds."""
        return compile_postgres_binds(sql, parameters)

    @staticmethod
    def normalize_parameters(parameters: Sequence[Any] | None) -> tuple[Any, ...]:
        """Copy mutable parameter buffers and FFI views into immutable owned values."""
        return normalize_postgres_parameters(parameters)


strip_pg_jsonb_header = PostgreSQLCore.strip_jsonb_header
encode_pg_jsonb = PostgreSQLCore.encode_jsonb
encode_pgcopy_rows = PostgreSQLCore.encode_pgcopy_rows
decode_pgcopy_rows = PostgreSQLCore.decode_pgcopy_rows
compile_pg_binds = PostgreSQLCore.compile_binds
normalize_pg_parameters = PostgreSQLCore.normalize_parameters
