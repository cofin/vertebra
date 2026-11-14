"""PostgreSQL binary JSONB and PGCOPY wire codecs."""

from __future__ import annotations

import struct
from collections.abc import Buffer, Callable, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

from vertebra.db.adapters.postgres.binds import (
    CompiledPostgresBinds,
    compile_postgres_binds,
    normalize_postgres_parameters,
)
from vertebra.db.adapters.postgres.options import build_native_client_options
from vertebra.db.adapters.postgres.types import PostgresArrowResult, PostgresQueryResult, _project_public_row
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import ProgrammingError
from vertebra.db.rls import TenantPredicate, TenantScopeConfig
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import encode_json_bytes
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.postgres.config import PostgreSQLConfig

__all__ = (
    "PGCOPY_BINARY_SIGNATURE",
    "PostgreSQLCore",
)

PGCOPY_BINARY_SIGNATURE: bytes = b"PGCOPY\n\xff\r\n\x00"

_DEFAULT_TENANT_SCOPE = TenantScopeConfig()


class PostgreSQLCore:
    """Protocol codecs, validation helpers, and wire primitives for PostgreSQL drivers."""

    DEFAULT_ARROW_MAX_RESULT_BYTES: Final[int] = 268_435_456
    DEFAULT_TENANT_SCOPE: Final[TenantScopeConfig] = _DEFAULT_TENANT_SCOPE

    @staticmethod
    def build_session_guc_sql(
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
        *,
        is_local: bool = True,
    ) -> tuple[str, tuple[str, str, bool]]:
        """Build a bound PostgreSQL session setting statement."""
        setting_name = PostgreSQLCore.validate_tenant_setting_name(config.setting_name)
        return "SELECT set_config($1, $2, $3)", (setting_name, tenant_value, is_local)

    @staticmethod
    def build_tenant_predicate_sql(predicate: TenantPredicate, *, param_index: int = 1) -> str:
        """Render a PostgreSQL numeric bind with the optional cast on its value."""
        suffix = f"::{predicate.config.parameter_cast}" if predicate.config.parameter_cast else ""
        return f"{predicate.column_reference()} = ${param_index}{suffix}"

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
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
        raw_arg = (
            bytes(value)
            if isinstance(value, (bytes, bytearray, memoryview))
            else (value.encode("utf-8") if isinstance(value, str) else encode_json_bytes(value))
        )
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            native_fn = getattr(native_mod, "encode_pg_jsonb", None)
            if callable(native_fn):
                return bytes(native_fn(raw_arg))

        if isinstance(value, (bytes, bytearray, memoryview)) and raw_arg.startswith(b"\x01"):
            return raw_arg
        return b"\x01" + raw_arg

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
        if native_mod is not None and flags == 0 and not ext_bytes:
            native_fn = getattr(native_mod, "encode_pgcopy_rows", None)
            if callable(native_fn):
                return bytes(native_fn(normalized_rows))

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

        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            native_fn = getattr(native_mod, "decode_pgcopy_rows", None)
            if callable(native_fn):
                try:
                    res = native_fn(bytes(raw))
                except ValueError as exc:
                    if "trailer" in str(exc):
                        msg = "Unexpected EOF before PGCOPY trailer"
                        raise ValueError(msg) from exc
                    raise
                return [tuple(None if field is None else bytes(field) for field in row) for row in res]

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

    @staticmethod
    def reject_memory_dsn(dsn: str) -> None:
        """Reject unsupported ``memory://`` DSNs cleanly with ``ImproperConfigurationError``."""
        if dsn.strip().lower().startswith("memory://"):
            msg = "PostgreSQL drivers do not support memory:// DSNs; pass a postgresql:// DSN or inject pool="
            raise ImproperConfigurationError(msg)

    @staticmethod
    def resolve_native_module() -> Any:
        """Resolve the compiled ``vertebra._postgres`` extension or raise ``MissingDependencyError``."""
        return ensure_module(
            "vertebra._postgres",
            package="vertebra-core[postgres]",
            extra="postgres",
        )

    @staticmethod
    def prepare_query(
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        """Compile SQL placeholders and normalize parameters for native execution and pipelining."""
        compiled = compile_postgres_binds(sql, parameters)
        return compiled.sql, compiled.params

    @staticmethod
    def project_query_rows(result: PostgresQueryResult) -> tuple[list[tuple[Any, ...]], tuple[str, ...], int]:
        """Convert a native ``PostgresQueryResult`` into ``(rows, column_names, affected_rows)``."""
        cols = tuple(col.name for col in result.columns)
        rows = [_project_public_row(row) for row in result.rows]
        return rows, cols, int(result.affected_rows)

    @staticmethod
    def arrow_result_to_cursor(result: PostgresArrowResult) -> ArrowCursor:
        """Wrap an inline IPC or native SHM ``PostgresArrowResult`` into ``ArrowCursor`` without row copies."""
        if result.used_shm:
            return ArrowCursor(
                result.buffer,
                columns=result.columns,
                used_shm=True,
                shm_cursor=result.buffer,
                row_count=result.row_count,
            )
        return ArrowCursor(
            result.buffer,
            columns=result.columns,
            used_shm=False,
            row_count=result.row_count,
        )

    @staticmethod
    def validate_arrow_target_identifiers(
        table_name: str,
        *,
        columns: Sequence[str] | None,
        schema_name: str | None,
    ) -> None:
        """Validate SQL identifiers passed to bulk Arrow COPY ingestion."""
        if schema_name is not None:
            validate_identifier(schema_name, "schema_name", allow_dots=False)
        validate_identifier(table_name, "table_name", allow_dots=schema_name is None)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)

    @staticmethod
    def coerce_arrow_source(data: Any, columns: Sequence[str] | None) -> Any | None:
        """Normalize plain row sequences into ``ArrowCursor`` payloads while preserving native Arrow inputs."""
        if (
            isinstance(data, Sequence)
            and not isinstance(data, (str, bytes, bytearray, memoryview, ArrowCursor))
            and not hasattr(data, "__arrow_c_stream__")
            and not hasattr(data, "to_batches")
        ):
            if not data and columns is None:
                return None
            return ArrowCursor.from_rows(data, columns)
        return data

    @staticmethod
    def validate_direct_session_config(config: PostgreSQLConfig, *, feature: str) -> PostgreSQLConfig:
        """Ensure direct non-pooled configuration is active before opening LISTEN or CDC streams."""
        direct_cfg = config.derive_direct_config()
        PostgreSQLCore.reject_memory_dsn(direct_cfg.dsn)
        if direct_cfg.pooler_mode not in ("direct", "session") or direct_cfg.pgbouncer:
            msg = (
                f"PostgreSQL {feature} requires a direct or session-mode connection "
                f"(got pooler_mode={direct_cfg.pooler_mode!r}); configure direct_dsn for pooled deployments"
            )
            raise ImproperConfigurationError(msg)
        return direct_cfg

    @staticmethod
    def build_direct_client_kwargs(config: PostgreSQLConfig, *, feature: str) -> dict[str, Any]:
        """Resolve dedicated non-pooled ``AsyncPostgresClient`` keyword arguments from ``config``."""
        direct_cfg = PostgreSQLCore.validate_direct_session_config(config, feature=feature)
        return build_native_client_options(direct_cfg).to_client_kwargs()

    @staticmethod
    def validate_tenant_setting_name(setting_name: str) -> str:
        """Validate a dotted PostgreSQL GUC setting name for tenant scoping."""
        if not setting_name:
            msg = "Tenant scope setting_name cannot be empty"
            raise ProgrammingError(msg)
        for segment in setting_name.split("."):
            validate_identifier(segment, "setting_name", allow_dots=False)
        return setting_name

    @staticmethod
    def validate_savepoint_token(token: int | None, allocator: Callable[[], int]) -> int:
        """Validate or allocate a numeric savepoint token."""
        if token is None:
            return allocator()
        if isinstance(token, bool) or not isinstance(token, int) or token < 0:
            msg = "Savepoint token must be a non-negative integer"
            raise ProgrammingError(msg)
        return token

    @staticmethod
    def chunk_iterable[ItemT](items: Iterable[ItemT], size: int) -> Iterator[list[ItemT]]:
        """Yield fixed-size slices from an arbitrary iterable without full buffering."""
        chunk_limit = max(1, int(size))
        buf: list[ItemT] = []
        for item in items:
            buf.append(item)
            if len(buf) >= chunk_limit:
                yield buf
                buf = []
        if buf:
            yield buf
