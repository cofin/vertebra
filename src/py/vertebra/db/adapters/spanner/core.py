"""Core protocol encoding, mutation batching, and Arrow/ObjectStore utilities for Cloud Spanner."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

import msgspec

from vertebra._vertebra import (
    SpannerError,
    hydrate_rows_vectorcall,
)
from vertebra.db.adapters.spanner.config import SpannerConfig
from vertebra.db.arrow import ArrowCursor, write_arrow_shm
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.telemetry.otel import TraceContext
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_schema
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.type_converters import time_iso_convert
from vertebra.utils.type_guards import get_msgspec_rename_config

if TYPE_CHECKING:
    from vertebra._spanner import (
        AsyncSpannerClient as _RustAsyncSpannerClient,
    )
    from vertebra._spanner import (
        SyncSpannerClient as _RustSyncSpannerClient,
    )

__all__ = ("SpannerCore",)

_PARTITIONED_QUERY_FALLBACK_CODES: Final[frozenset[int]] = frozenset({3, 7, 9, 12, 14})


class SpannerCore:
    """Stateless protocol encoding, mutation chunking, and Arrow/ObjectStore helpers for Cloud Spanner."""

    @staticmethod
    def create_sync_rust_client(config: SpannerConfig) -> _RustSyncSpannerClient:
        """Instantiate the native Rust `SyncSpannerClient` from `config`."""
        spanner_mod = ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")
        client: _RustSyncSpannerClient = spanner_mod.SyncSpannerClient(
            database=config.database_uri,
            database_role=config.database_role,
            endpoint=config.endpoint,
            dialect=config.dialect,
            pool_size=max(1, config.pool_size),
            route_to_leader_enabled=config.route_to_leader_enabled,
            max_commit_delay_ms=config.max_commit_delay_ms,
            auth_options=config.build_auth_options() or None,
            admin_endpoint=config.admin_endpoint,
            ca_pem=config.ca_pem,
        )
        return client

    @staticmethod
    def create_async_rust_client(config: SpannerConfig) -> _RustAsyncSpannerClient:
        """Instantiate the native Rust `AsyncSpannerClient` from `config`."""
        spanner_mod = ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")
        client: _RustAsyncSpannerClient = spanner_mod.AsyncSpannerClient(
            database=config.database_uri,
            database_role=config.database_role,
            endpoint=config.endpoint,
            dialect=config.dialect,
            pool_size=max(1, config.pool_size),
            route_to_leader_enabled=config.route_to_leader_enabled,
            max_commit_delay_ms=config.max_commit_delay_ms,
            auth_options=config.build_auth_options() or None,
            admin_endpoint=config.admin_endpoint,
            ca_pem=config.ca_pem,
        )
        return client

    @staticmethod
    def is_ddl_statement(sql: str) -> bool:
        """Return whether `sql` is a Spanner schema DDL statement (`CREATE`, `ALTER`, `DROP`)."""
        upper = sql.strip().upper()
        return upper.startswith(("CREATE ", "ALTER ", "DROP "))

    @staticmethod
    def build_select_sql_if_table(sql_or_table: str) -> str:
        """Wrap a table identifier in `SELECT * FROM ...` or return a SQL query unchanged."""
        stripped = sql_or_table.strip()
        upper = stripped.upper()
        if upper.startswith(("SELECT ", "WITH ", "GRAPH ")):
            return stripped
        return f"SELECT * FROM {stripped}"

    @staticmethod
    def is_partitioned_query_fallback_error(exc: BaseException) -> bool:
        """Return whether a `PartitionQuery` error should fall back to `ExecuteStreamingSql`.

        Matches canonical gRPC status codes on `SpannerError`: `3` (`INVALID_ARGUMENT`,
        non-partitionable query), `7` (`PERMISSION_DENIED`, Data Boost not permitted),
        `9` (`FAILED_PRECONDITION`, failed precondition), `12` (`UNIMPLEMENTED`,
        `PartitionQuery` unimplemented), and `14` (`UNAVAILABLE`, endpoint unavailable).
        """
        return isinstance(exc, SpannerError) and getattr(exc, "code", None) in _PARTITIONED_QUERY_FALLBACK_CODES

    @classmethod
    def encode_parameters_json(
        cls,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
        *,
        dialect: str = "googlesql",
    ) -> str | None:
        """Serialize positional or named Spanner parameters into a JSON object for the Rust client."""
        if parameters is None:
            return None
        if isinstance(parameters, Mapping):
            if not parameters:
                return None
            out: dict[str, Any] = {}
            for k, v in parameters.items():
                clean_key = str(k).lstrip("@$")
                out[clean_key] = cls._encode_param_entry(v)
            return encode_json(out)
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
            if not parameters:
                return None
            _ = dialect
            out_seq = {f"p{idx + 1}": cls._encode_param_entry(v) for idx, v in enumerate(parameters)}
            return encode_json(out_seq)
        return None

    @classmethod
    def encode_batch_dml_json(
        cls,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        dialect: str = "googlesql",
    ) -> str:
        """Serialize a batch of DML statements and parameters into a JSON array."""
        payload: list[dict[str, Any]] = []
        for item in statements:
            if isinstance(item, Mapping):
                sql = str(item.get("sql", ""))
                raw_params = item.get("params") or item.get("parameters")
            else:
                sql = str(item[0])
                raw_params = item[1]
            params_json = cls.encode_parameters_json(raw_params, dialect=dialect)
            entry: dict[str, Any] = {"sql": sql}
            if params_json is not None:
                entry["params"] = decode_json(params_json)
            payload.append(entry)
        return encode_json(payload)

    @staticmethod
    def hydrate_query_rows(
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Hydrate query rows into dictionaries or `schema_type` instances via C-API vectorcall or `to_schema`."""
        if not rows:
            return []
        col_tuple = tuple(columns)
        if schema_type is None or schema_type is dict:
            return [dict(zip(col_tuple, row, strict=False)) for row in rows]
        if get_msgspec_rename_config(schema_type) is None:
            try:
                return hydrate_rows_vectorcall(schema_type, list(rows), col_tuple)
            except Exception:
                pass
        dicts = [dict(zip(col_tuple, row, strict=False)) for row in rows]
        try:
            return list(to_schema(dicts, schema_type=schema_type))
        except Exception:
            if issubclass(schema_type, msgspec.Struct):
                return [msgspec.convert(d, type=schema_type) for d in dicts]
            return [schema_type(**d) for d in dicts]

    @staticmethod
    def annotate_sql_with_trace(sql: str, propagator: SqlTracePropagator) -> str:
        """Append W3C `sqlcommenter` trace context when an active span is present."""
        trace = TraceContext.current()
        if trace is None:
            return sql
        return propagator.append_sqlcommenter(
            sql,
            trace,
            db_driver="vertebra-spanner",
        )

    @staticmethod
    def coerce_arrow_to_dicts(data: ArrowCursor | bytes | Any) -> list[dict[str, Any]]:
        """Convert an `ArrowCursor`, Arrow IPC buffer, or table into a list of row dictionaries."""
        if isinstance(data, ArrowCursor):
            return data.to_pylist()
        if isinstance(data, (bytes, bytearray, memoryview)):
            return ArrowCursor(bytes(data)).to_pylist()
        if hasattr(data, "to_pylist"):
            return list(data.to_pylist())
        if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
            return [dict(item) for item in data if isinstance(item, Mapping)]
        ipc_view = write_arrow_shm(data)
        return ArrowCursor(bytes(ipc_view)).to_pylist()

    @classmethod
    def _encode_param_entry(cls, val: Any) -> Any:
        if val is None or isinstance(val, (bool, int, float, str)):
            return val
        if isinstance(val, Decimal):
            return {"type": "NUMERIC", "value": str(val)}
        if isinstance(val, (bytes, bytearray, memoryview)):
            return {
                "type": "BYTES",
                "value": base64.b64encode(bytes(val)).decode("ascii"),
            }
        if isinstance(val, datetime):
            return {"type": "TIMESTAMP", "value": time_iso_convert(val)}
        if isinstance(val, date):
            return {"type": "DATE", "value": time_iso_convert(val)}
        if isinstance(val, (list, tuple)):
            return [cls._encode_param_entry(item) for item in val]
        if isinstance(val, Mapping):
            if "type" in val and "value" in val:
                return dict(val)
            return {"type": "JSON", "value": encode_json(val)}
        return str(val)
