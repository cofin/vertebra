"""Core protocol encoding, mutation batching, and Arrow/ObjectStore utilities for Cloud Spanner."""

from __future__ import annotations

import base64
import csv
import inspect
import io
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlparse

import msgspec

from vertebra._vertebra import (
    SpannerError,
    hydrate_rows_vectorcall,
)
from vertebra.db.adapters.spanner.config import (
    Mutation,
    MutationGroup,
    SpannerConfig,
)
from vertebra.db.arrow import ArrowCursor, write_arrow_shm
from vertebra.storage import AsyncObjectStore, SyncObjectStore
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
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

_MAX_MUTATION_CELLS_PER_BATCH: Final[int] = 80_000
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

    @staticmethod
    def coerce_mutation(item: Mutation | Mapping[str, Any]) -> Mutation:
        """Coerce a `Mutation` or dictionary representation into a typed `Mutation` struct."""
        if isinstance(item, Mutation):
            return item
        op_raw = str(item.get("op", "insert_or_update")).lower()
        table = str(item.get("table", ""))
        if op_raw == "delete":
            return Mutation.delete(
                table,
                item.get("keys", ()),
                all_rows=bool(item.get("all", item.get("all_rows", False))),
            )
        cols = [str(c) for c in item.get("columns", ())]
        vals = [tuple(r) for r in item.get("values", ())]
        if op_raw == "insert":
            return Mutation.insert(table, cols, vals)
        if op_raw == "update":
            return Mutation.update(table, cols, vals)
        if op_raw == "replace":
            return Mutation.replace(table, cols, vals)
        return Mutation.insert_or_update(table, cols, vals)

    @classmethod
    def coerce_mutation_group(
        cls,
        group: MutationGroup | Sequence[Mutation | Mapping[str, Any]],
    ) -> MutationGroup:
        """Coerce a `MutationGroup` or sequence of mutations into a `MutationGroup` struct."""
        if isinstance(group, MutationGroup):
            return group
        return MutationGroup(mutations=tuple(cls.coerce_mutation(m) for m in group))

    @staticmethod
    def chunk_mutations_by_cell_limit(
        mutations: Sequence[Mutation],
        *,
        max_cells: int = _MAX_MUTATION_CELLS_PER_BATCH,
    ) -> list[list[Mutation]]:
        """Partition `mutations` into batches that respect Spanner's per-commit cell limit."""
        batches: list[list[Mutation]] = []
        current: list[Mutation] = []
        current_cells = 0
        for mut in mutations:
            cells = mut.cell_count()
            if current and current_cells + cells > max_cells:
                batches.append(current)
                current = []
                current_cells = 0
            current.append(mut)
            current_cells += cells
        if current:
            batches.append(current)
        return batches

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
    def write_cursor_to_object_store_sync(
        cls,
        cursor: ArrowCursor,
        *,
        destination_uri: str,
        store: Any | None,
        format: str,
        compression: str,
    ) -> None:
        """Write an `ArrowCursor` into a multicloud `ObjectStore` or structural storage backend."""
        if store is not None and hasattr(store, "write_arrow_cursor_sync"):
            object_key = cls._extract_object_key_from_uri(destination_uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"json", "jsonl", "ndjson", "csv"} and hasattr(store, "write_text_sync"):
                lines = [encode_json(row) for row in cursor.to_pylist()]
                store.write_text_sync(object_key, "\n".join(lines))
                return
            comp = None if compression.upper() in {"NONE", "UNCOMPRESSED"} else compression.upper()
            store.write_arrow_cursor_sync(object_key, cursor, file_format=file_fmt, compression=comp)
            return
        resolved_store, object_key = cls._resolve_sync_store_and_key(store, destination_uri)
        payload = cls._serialize_cursor_for_object_store(cursor, format=format, compression=compression)
        put_fn = getattr(resolved_store, "put", None)
        if callable(put_fn):
            put_fn(object_key, payload)
            return
        raise ImproperConfigurationError("export_to_object_store requires a valid ObjectStore or URI")

    @classmethod
    async def write_cursor_to_object_store_async(
        cls,
        cursor: ArrowCursor,
        *,
        destination_uri: str,
        store: Any | None,
        format: str,
        compression: str,
    ) -> None:
        """Asynchronously write an `ArrowCursor` into a multicloud `ObjectStore` or structural storage backend."""
        if store is not None and hasattr(store, "write_arrow_cursor_async"):
            object_key = cls._extract_object_key_from_uri(destination_uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"json", "jsonl", "ndjson", "csv"} and hasattr(store, "write_text_async"):
                lines = [encode_json(row) for row in cursor.to_pylist()]
                await store.write_text_async(object_key, "\n".join(lines))
                return
            comp = None if compression.upper() in {"NONE", "UNCOMPRESSED"} else compression.upper()
            await store.write_arrow_cursor_async(object_key, cursor, file_format=file_fmt, compression=comp)
            return
        resolved_store, object_key = cls._resolve_async_store_and_key(store, destination_uri)
        payload = cls._serialize_cursor_for_object_store(cursor, format=format, compression=compression)
        put_fn = getattr(resolved_store, "put", None)
        if callable(put_fn):
            res = put_fn(object_key, payload)
            if inspect.isawaitable(res):
                await res
            return
        raise ImproperConfigurationError("export_to_object_store requires a valid ObjectStore or URI")

    @classmethod
    def read_cursor_from_object_store_sync(
        cls,
        uri: str,
        *,
        store: Any | None,
        format: str,
    ) -> ArrowCursor:
        """Read an `ArrowCursor` from a multicloud `ObjectStore` or structural storage backend."""
        if store is not None and hasattr(store, "read_arrow_sync"):
            object_key = cls._extract_object_key_from_uri(uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"json", "jsonl", "ndjson", "csv"} and hasattr(store, "read_text_sync"):
                raw_text = store.read_text_sync(object_key)
                dicts = [decode_json(line) for line in raw_text.splitlines() if line.strip()]
                if not dicts:
                    return ArrowCursor.from_rows([], columns=[])
                cols = list(dicts[0].keys())
                rows = [[d.get(c) for c in cols] for d in dicts]
                return ArrowCursor.from_rows(rows, columns=cols)
            res = store.read_arrow_sync(object_key, file_format=file_fmt)
            if isinstance(res, ArrowCursor):
                return res
            return ArrowCursor(bytes(write_arrow_shm(res)))
        resolved_store, object_key = cls._resolve_sync_store_and_key(store, uri)
        get_fn = getattr(resolved_store, "get_bytes", None) or getattr(resolved_store, "get", None)
        if callable(get_fn):
            raw = bytes(get_fn(object_key))
            return cls._deserialize_bytes_from_object_store(raw, format=format)
        raise ImproperConfigurationError("load_from_object_store requires a valid ObjectStore or URI")

    @classmethod
    async def read_cursor_from_object_store_async(
        cls,
        uri: str,
        *,
        store: Any | None,
        format: str,
    ) -> ArrowCursor:
        """Asynchronously read an `ArrowCursor` from a multicloud `ObjectStore` or structural storage backend."""
        if store is not None and hasattr(store, "read_arrow_async"):
            object_key = cls._extract_object_key_from_uri(uri)
            file_fmt = cls._normalize_backend_file_format(format)
            if file_fmt in {"json", "jsonl", "ndjson", "csv"} and hasattr(store, "read_text_async"):
                raw_text = await store.read_text_async(object_key)
                dicts = [decode_json(line) for line in raw_text.splitlines() if line.strip()]
                if not dicts:
                    return ArrowCursor.from_rows([], columns=[])
                cols = list(dicts[0].keys())
                rows = [[d.get(c) for c in cols] for d in dicts]
                return ArrowCursor.from_rows(rows, columns=cols)
            res = await store.read_arrow_async(object_key, file_format=file_fmt)
            if isinstance(res, ArrowCursor):
                return res
            return ArrowCursor(bytes(write_arrow_shm(res)))
        resolved_store, object_key = cls._resolve_async_store_and_key(store, uri)
        get_fn = getattr(resolved_store, "get_bytes", None) or getattr(resolved_store, "get", None)
        if callable(get_fn):
            res = get_fn(object_key)
            if inspect.isawaitable(res):
                res = await res
            raw = bytes(res)
            return cls._deserialize_bytes_from_object_store(raw, format=format)
        raise ImproperConfigurationError("load_from_object_store requires a valid ObjectStore or URI")

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

    @staticmethod
    def _extract_object_key_from_uri(uri: str) -> str:
        if "://" not in uri:
            return uri.lstrip("/")
        parsed = urlparse(uri)
        return parsed.path.lstrip("/")

    @staticmethod
    def _serialize_cursor_for_object_store(
        cursor: ArrowCursor,
        *,
        format: str,
        compression: str,
    ) -> bytes:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return bytes(cursor.buffer_view())
        if fmt == "parquet":
            pa = import_optional("pyarrow")
            pq = import_optional("pyarrow.parquet")
            if pa is not None and pq is not None:
                tbl = cursor.to_arrow_table()
                sink = pa.BufferOutputStream()
                comp = None if compression.upper() in {"NONE", "UNCOMPRESSED"} else compression.upper()
                pq.write_table(tbl, sink, compression=comp)
                return bytes(sink.getvalue().to_pybytes())
        if fmt == "csv":
            rows = cursor.to_pylist()
            if not rows:
                return b""
            cols = list(rows[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            return buf.getvalue().encode("utf-8")
        rows = cursor.to_pylist()
        return ("\n".join(encode_json(r) for r in rows) + ("\n" if rows else "")).encode("utf-8")

    @staticmethod
    def _deserialize_bytes_from_object_store(
        payload: bytes,
        *,
        format: str,
    ) -> ArrowCursor:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return ArrowCursor(payload)
        if fmt == "parquet":
            pa = import_optional("pyarrow")
            pq = import_optional("pyarrow.parquet")
            if pa is not None and pq is not None:
                tbl = pq.read_table(pa.py_buffer(payload))
                ipc_view = write_arrow_shm(tbl)
                return ArrowCursor(bytes(ipc_view))
        if fmt in {"json", "jsonl", "ndjson"}:
            lines = [ln.strip() for ln in payload.decode("utf-8", errors="replace").splitlines() if ln.strip()]
            dicts = [decode_json(ln) for ln in lines]
            if not dicts:
                return ArrowCursor.from_rows([], columns=[])
            cols = list(dicts[0].keys())
            rows = [[d.get(c) for c in cols] for d in dicts]
            return ArrowCursor.from_rows(rows, columns=cols)
        if fmt == "csv":
            text = payload.decode("utf-8", errors="replace")
            if not text.strip():
                return ArrowCursor.from_rows([], columns=[])
            reader = csv.DictReader(io.StringIO(text))
            dicts = list(reader)
            cols = list(reader.fieldnames or ())
            rows = [[d.get(c) for c in cols] for d in dicts]
            return ArrowCursor.from_rows(rows, columns=cols)
        return ArrowCursor(payload)

    @classmethod
    def _resolve_sync_store_and_key(cls, store: Any | None, uri: str) -> tuple[Any, str]:
        if isinstance(store, SyncObjectStore):
            return store, cls._resolve_store_key(store, uri)
        if isinstance(store, AsyncObjectStore):
            sync_store = store.to_sync()
            return sync_store, cls._resolve_store_key(sync_store, uri)
        if isinstance(store, str):
            sync_store = SyncObjectStore(store)
            return sync_store, cls._resolve_store_key(sync_store, uri)
        if store is not None:
            if hasattr(store, "to_url_and_options"):
                sync_store = SyncObjectStore.from_config(store)
                return sync_store, cls._resolve_store_key(sync_store, uri)
            return store, cls._resolve_store_key(store, uri)
        if "://" in uri:
            parsed = urlparse(uri)
            if parsed.scheme == "file":
                parent_dir = os.path.dirname(parsed.path) or "/"
                return SyncObjectStore.from_local(parent_dir, create_dirs=True), os.path.basename(parsed.path)
            if parsed.scheme == "memory":
                key = f"{parsed.netloc}/{parsed.path.lstrip('/')}".strip("/")
                return SyncObjectStore("memory://"), key
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            return SyncObjectStore(base_url), parsed.path.lstrip("/")
        raise ImproperConfigurationError("export_to_object_store requires a valid ObjectStore or URI")

    @classmethod
    def _resolve_async_store_and_key(cls, store: Any | None, uri: str) -> tuple[Any, str]:
        if isinstance(store, AsyncObjectStore):
            return store, cls._resolve_store_key(store, uri)
        if isinstance(store, SyncObjectStore):
            if getattr(store, "scheme", "") == "memory":
                return store, cls._resolve_store_key(store, uri)
            async_store = store.to_async()
            return async_store, cls._resolve_store_key(async_store, uri)
        if isinstance(store, str):
            async_store = AsyncObjectStore(store)
            return async_store, cls._resolve_store_key(async_store, uri)
        if store is not None:
            if hasattr(store, "to_url_and_options"):
                async_store = AsyncObjectStore.from_config(store)
                return async_store, cls._resolve_store_key(async_store, uri)
            return store, cls._resolve_store_key(store, uri)
        if "://" in uri:
            parsed = urlparse(uri)
            if parsed.scheme == "file":
                parent_dir = os.path.dirname(parsed.path) or "/"
                return AsyncObjectStore.from_local(parent_dir, create_dirs=True), os.path.basename(parsed.path)
            if parsed.scheme == "memory":
                key = f"{parsed.netloc}/{parsed.path.lstrip('/')}".strip("/")
                return AsyncObjectStore("memory://"), key
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            return AsyncObjectStore(base_url), parsed.path.lstrip("/")
        raise ImproperConfigurationError("export_to_object_store requires a valid ObjectStore or URI")

    @staticmethod
    def _resolve_store_key(store: Any, uri: str) -> str:
        if "://" not in uri:
            return uri.lstrip("/")
        store_url = getattr(store, "url", None)
        if isinstance(store_url, str) and store_url:
            prefix = store_url.rstrip("/") + "/"
            if uri.startswith(prefix):
                return uri[len(prefix) :].lstrip("/")
        parsed = urlparse(uri)
        if parsed.scheme == "memory":
            return f"{parsed.netloc}/{parsed.path.lstrip('/')}".strip("/")
        return parsed.path.lstrip("/")

    @staticmethod
    def _normalize_backend_file_format(format: str) -> str:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return "arrow-ipc"
        return fmt
