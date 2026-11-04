"""Core protocol encoding, identifier parsing, and Arrow/ObjectStore helpers for BigQuery."""

from __future__ import annotations

import base64
import inspect
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Final
from urllib.parse import urlparse

import msgspec

from vertebra._vertebra import (
    BigQueryError,
    hydrate_rows_vectorcall,
)
from vertebra.db.adapters.bigquery.config import BigQueryConfig, BigQueryDryRunResult, ObjectTableConfig
from vertebra.db.arrow import ArrowCursor, write_arrow_shm
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.schema import to_schema
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.text import split_qualified_identifier
from vertebra.utils.type_converters import time_iso_convert
from vertebra.utils.type_guards import get_msgspec_rename_config

if TYPE_CHECKING:
    from vertebra._bigquery import (
        AsyncBigQueryClient as _RustAsyncBigQueryClient,
    )
    from vertebra._bigquery import (
        SyncBigQueryClient as _RustSyncBigQueryClient,
    )

__all__ = ("BigQueryCore",)

_ON_DEMAND_TIB_PRICE_USD: Final[float] = 6.25
_BYTES_PER_TIB: Final[float] = float(1 << 40)
_STORAGE_API_FALLBACK_CODES: Final[frozenset[int]] = frozenset({7, 9, 12, 14})


class BigQueryCore:
    """Stateless protocol encoding, identifier parsing, and Arrow/ObjectStore utilities for BigQuery."""

    FINOPS_METRICS: ClassVar[dict[str, int]] = {
        "bytes_billed": 0,
        "slot_ms": 0,
        "cache_hit": 0,
    }

    @classmethod
    def create_sync_rust_client(cls, config: BigQueryConfig) -> _RustSyncBigQueryClient:
        """Instantiate the native Rust `SyncBigQueryClient` from `config`."""
        bq_mod = ensure_module("vertebra._bigquery", package="vertebra-core[bigquery]", extra="bigquery")
        project_id = config.effective_project_id()
        dataset_id = config.effective_dataset_id()
        sanitized_labels = cls.sanitize_labels(config.labels, None)
        auth_options = config.build_auth_options()

        client: _RustSyncBigQueryClient = bq_mod.SyncBigQueryClient(
            project_id=project_id,
            dataset_id=dataset_id,
            location=config.location,
            rest_endpoint=config.rest_endpoint,
            storage_endpoint=config.storage_endpoint,
            maximum_bytes_billed=config.maximum_bytes_billed,
            job_timeout_ms=config.job_timeout_ms,
            priority=config.priority,
            labels=sanitized_labels or None,
            auth_options=auth_options or None,
            pool_size=max(1, config.pool_size),
            ca_pem=config.ca_pem,
        )
        return client

    @classmethod
    def create_async_rust_client(cls, config: BigQueryConfig) -> _RustAsyncBigQueryClient:
        """Instantiate the native Rust `AsyncBigQueryClient` from `config`."""
        bq_mod = ensure_module("vertebra._bigquery", package="vertebra-core[bigquery]", extra="bigquery")
        project_id = config.effective_project_id()
        dataset_id = config.effective_dataset_id()
        sanitized_labels = cls.sanitize_labels(config.labels, None)
        auth_options = config.build_auth_options()

        client: _RustAsyncBigQueryClient = bq_mod.AsyncBigQueryClient(
            project_id=project_id,
            dataset_id=dataset_id,
            location=config.location,
            rest_endpoint=config.rest_endpoint,
            storage_endpoint=config.storage_endpoint,
            maximum_bytes_billed=config.maximum_bytes_billed,
            job_timeout_ms=config.job_timeout_ms,
            priority=config.priority,
            labels=sanitized_labels or None,
            auth_options=auth_options or None,
            pool_size=max(1, config.pool_size),
            ca_pem=config.ca_pem,
        )
        return client

    @classmethod
    def encode_parameters_json(
        cls,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
    ) -> str | None:
        """Serialize positional or named BigQuery parameters into a JSON array for the Rust client."""
        if parameters is None:
            return None
        if isinstance(parameters, Mapping):
            if not parameters:
                return None
            encoded = [cls._encode_param_descriptor(val, name=str(key)) for key, val in parameters.items()]
            return encode_json(encoded)
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
            if not parameters:
                return None
            encoded_seq = [cls._encode_param_descriptor(item) for item in parameters]
            return encode_json(encoded_seq)
        return None

    @staticmethod
    def rows_to_dicts(
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> list[dict[str, Any]]:
        """Convert tabular rows and column names into a list of dictionaries."""
        col_names = list(columns)
        return [dict(zip(col_names, row, strict=False)) for row in rows]

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
    def build_dry_run_result(raw: Mapping[str, Any]) -> BigQueryDryRunResult:
        """Build a `BigQueryDryRunResult` from raw dry-run response statistics."""
        processed = int(raw.get("total_bytes_processed", 0))
        billed = int(raw.get("total_bytes_billed", processed))
        cache_hit = bool(raw.get("cache_hit", False))
        ref_tables = tuple(str(t) for t in raw.get("referenced_tables", ()))
        schema_items = tuple(dict(f) for f in raw.get("schema", ()))
        estimated_cost = 0.0 if cache_hit else (max(0, billed) / _BYTES_PER_TIB) * _ON_DEMAND_TIB_PRICE_USD
        return BigQueryDryRunResult(
            total_bytes_processed=processed,
            total_bytes_billed=billed,
            cache_hit=cache_hit,
            referenced_tables=ref_tables,
            schema=schema_items,
            estimated_cost_usd=round(estimated_cost, 8),
        )

    @classmethod
    def sanitize_labels(
        cls,
        base_labels: Mapping[str, str] | None,
        override_labels: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """Merge and sanitize BigQuery job labels to conform to GCP label naming rules."""
        combined: dict[str, str] = {}
        for source in (base_labels, override_labels):
            if not source:
                continue
            for k, v in source.items():
                if len(combined) >= 64:
                    break
                key = cls._sanitize_label_component(str(k), allow_empty=False)
                val = cls._sanitize_label_component(str(v), allow_empty=True)
                combined[key] = val
        return combined

    @staticmethod
    def annotate_sql_with_trace(sql: str, propagator: SqlTracePropagator) -> str:
        """Append W3C `sqlcommenter` trace context when an active span is present."""
        trace = TraceContext.current()
        if trace is None:
            return sql
        return propagator.append_sqlcommenter(
            sql,
            trace,
            db_driver="vertebra-bigquery",
        )

    @classmethod
    def record_finops_metrics(
        cls,
        project_id: str,
        location: str,
        raw: Mapping[str, Any],
    ) -> None:
        """Accumulate BigQuery FinOps billing and slot telemetry counters."""
        _ = (project_id, location)
        bytes_billed = int(raw.get("total_bytes_billed", 0))
        slot_ms = int(raw.get("slot_ms", 0))
        cache_hit = bool(raw.get("cache_hit", False))
        cls.FINOPS_METRICS["bytes_billed"] += max(0, bytes_billed)
        cls.FINOPS_METRICS["slot_ms"] += max(0, slot_ms)
        if cache_hit:
            cls.FINOPS_METRICS["cache_hit"] += 1

    @staticmethod
    def split_table_parts(raw: str) -> list[str]:
        """Split a BigQuery table reference into `[project, dataset, table]`, `[dataset, table]`, or `[table]`."""
        cleaned = raw.strip().replace("`", "")
        if not cleaned:
            return []
        if ":" in cleaned:
            first, rest = cleaned.split(":", 1)
            if "." in first:
                if ":" in rest:
                    proj_slug, ds_and_table = rest.split(":", 1)
                    return [f"{first}:{proj_slug}", *split_qualified_identifier(ds_and_table)]
                dot_parts = split_qualified_identifier(rest)
                if dot_parts:
                    return [f"{first}:{dot_parts[0]}", *dot_parts[1:]]
                return [cleaned]
            return [first, *split_qualified_identifier(rest)]
        return split_qualified_identifier(cleaned)

    @classmethod
    def try_parse_table_reference(
        cls,
        sql_or_table: str,
        *,
        default_project: str,
        default_dataset: str | None,
    ) -> tuple[str, str, str] | None:
        """Parse a table identifier into `(project_id, dataset_id, table_id)` or `None` if it is a SQL query."""
        stripped = sql_or_table.strip().strip("`")
        if not stripped or any(ch.isspace() for ch in stripped):
            return None
        parts = cls.split_table_parts(stripped)
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return default_project, parts[0], parts[1]
        if len(parts) == 1 and default_dataset:
            return default_project, default_dataset, parts[0]
        return None

    @classmethod
    def qualify_table_resource(
        cls,
        table: str,
        *,
        default_project: str,
        default_dataset: str | None,
    ) -> str:
        """Format a table identifier as `projects/{p}/datasets/{d}/tables/{t}`."""
        stripped = table.strip().strip("`")
        if stripped.startswith("projects/") and "/datasets/" in stripped and "/tables/" in stripped:
            return stripped
        parts = cls.split_table_parts(stripped)
        if len(parts) == 3:
            return f"projects/{parts[0]}/datasets/{parts[1]}/tables/{parts[2]}"
        if len(parts) == 2:
            return f"projects/{default_project}/datasets/{parts[0]}/tables/{parts[1]}"
        if len(parts) == 1 and default_dataset:
            return f"projects/{default_project}/datasets/{default_dataset}/tables/{parts[0]}"
        raise ImproperConfigurationError(
            f"Unqualified BigQuery table {table!r} requires BigQueryConfig.dataset_id or 'dataset.table'"
        )

    @staticmethod
    def is_storage_api_fallback_error(exc: BaseException) -> bool:
        """Return whether a Storage Read/Write gRPC error indicates the Storage API is unavailable or disabled.

        Matches canonical gRPC status codes on `BigQueryError`: `7` (`PERMISSION_DENIED`),
        `9` (`FAILED_PRECONDITION`), `12` (`UNIMPLEMENTED`), and `14` (`UNAVAILABLE`).
        """
        return isinstance(exc, BigQueryError) and getattr(exc, "code", None) in _STORAGE_API_FALLBACK_CODES

    @staticmethod
    def build_table_or_query_sql(
        sql_or_table: str,
        *,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
    ) -> str:
        """Wrap a table identifier in a `SELECT` statement or return a SQL query unchanged."""
        stripped = sql_or_table.strip()
        upper = stripped.upper()
        if upper.startswith(("SELECT ", "WITH ", "(SELECT ")):
            return stripped
        cols = ", ".join(selected_fields) if selected_fields else "*"
        where_clause = f" WHERE {row_restriction}" if row_restriction else ""
        return f"SELECT {cols} FROM {stripped}{where_clause}"

    @staticmethod
    def query_result_to_arrow_cursor(raw: Mapping[str, Any]) -> ArrowCursor:
        """Convert a REST `jobs.query` response dictionary into an `ArrowCursor`."""
        columns = [str(c) for c in raw.get("columns", ())]
        rows = list(raw.get("rows", ()))
        return ArrowCursor.from_rows(rows, columns=columns, threshold_rows=1)

    @staticmethod
    def coerce_arrow_ipc_bytes(data: ArrowCursor | bytes | Any) -> bytes:
        """Extract serialized Arrow IPC stream bytes from an `ArrowCursor`, buffer, or table."""
        if isinstance(data, ArrowCursor):
            return bytes(data.buffer_view())
        if isinstance(data, (bytes, bytearray, memoryview)):
            return bytes(data)
        return bytes(write_arrow_shm(data))

    @staticmethod
    def arrow_ipc_bytes_to_dicts(ipc_bytes: bytes) -> list[dict[str, Any]]:
        """Decode Arrow IPC stream bytes into a list of row dictionaries."""
        cursor = ArrowCursor(ipc_bytes)
        return cursor.to_pylist()

    @classmethod
    def build_export_data_sql(
        cls,
        sql_or_table: str,
        *,
        destination_uri: str,
        format: str,
        compression: str,
        overwrite: bool,
    ) -> str:
        """Build a GoogleSQL `EXPORT DATA OPTIONS(...) AS SELECT ...` statement."""
        query_sql = cls.build_table_or_query_sql(sql_or_table)
        fmt_upper = format.strip().upper()
        if fmt_upper == "NDJSON":
            fmt_upper = "JSON"
        comp_upper = compression.strip().upper()
        ov_str = "true" if overwrite else "false"
        escaped_uri = destination_uri.replace("'", "\\'")
        return (
            "EXPORT DATA OPTIONS(\n"
            f"  uri = '{escaped_uri}',\n"
            f"  format = '{fmt_upper}',\n"
            f"  compression = '{comp_upper}',\n"
            f"  overwrite = {ov_str}\n"
            ") AS\n"
            f"{query_sql}"
        )

    @staticmethod
    def build_load_data_sql(
        table: str,
        *,
        uris: Sequence[str],
        format: str,
        write_disposition: str,
    ) -> str:
        """Build a GoogleSQL `LOAD DATA [INTO|OVERWRITE] ... FROM FILES(...)` statement."""
        fmt_upper = format.strip().upper()
        if fmt_upper == "NDJSON":
            fmt_upper = "JSON"
        verb = "OVERWRITE" if write_disposition == "WRITE_TRUNCATE" else "INTO"
        escaped_uris = ", ".join("'" + u.replace("'", "\\'") + "'" for u in uris)
        return f"LOAD DATA {verb} {table}\nFROM FILES (\n  format = '{fmt_upper}',\n  uris = [{escaped_uris}]\n)"

    @staticmethod
    def build_object_table_select_sql(
        table_name: str,
        *,
        where: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Build a `SELECT` query against a BigQuery Object Table."""
        sql = f"SELECT uri, generation, content_type, size, md5_hash, updated FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return sql

    @classmethod
    def write_arrow_cursor_to_object_store_sync(
        cls,
        cursor: ArrowCursor,
        *,
        destination_uri: str,
        store: Any | None,
        format: str,
        compression: str,
    ) -> None:
        """Write an `ArrowCursor` into a multicloud `ObjectStore` or `ObjectStoreBackend`."""
        backend = cls._resolve_object_store_backend(store, destination_uri)
        if backend is not None:
            object_key = ObjectTableConfig._extract_object_key_from_uri(destination_uri)
            if hasattr(backend, "write_arrow_cursor_sync"):
                file_fmt = cls._normalize_backend_file_format(format)
                comp = None if compression.upper() in {"NONE", "UNCOMPRESSED"} else compression.upper()
                backend.write_arrow_cursor_sync(object_key, cursor, file_format=file_fmt, compression=comp)
                return
        if store is None:
            raise ImproperConfigurationError(
                "Non-GCS export_to_object_store requires an ObjectStore instance or ObjectStoreBackend"
            )
        object_key = ObjectTableConfig._extract_object_key_from_uri(destination_uri)
        payload = cls._serialize_cursor_for_object_store(cursor, format=format, compression=compression)
        put_fn = getattr(store, "put", None)
        if callable(put_fn):
            put_fn(object_key, payload)
            return
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(store).__name__!r}")

    @classmethod
    async def write_arrow_cursor_to_object_store_async(
        cls,
        cursor: ArrowCursor,
        *,
        destination_uri: str,
        store: Any | None,
        format: str,
        compression: str,
    ) -> None:
        """Asynchronously write an `ArrowCursor` into a multicloud `ObjectStore` or `ObjectStoreBackend`."""
        backend = cls._resolve_object_store_backend(store, destination_uri)
        if backend is not None:
            object_key = ObjectTableConfig._extract_object_key_from_uri(destination_uri)
            if hasattr(backend, "write_arrow_cursor_async"):
                file_fmt = cls._normalize_backend_file_format(format)
                comp = None if compression.upper() in {"NONE", "UNCOMPRESSED"} else compression.upper()
                await backend.write_arrow_cursor_async(object_key, cursor, file_format=file_fmt, compression=comp)
                return
        if store is None:
            raise ImproperConfigurationError(
                "Non-GCS export_to_object_store requires an ObjectStore instance or ObjectStoreBackend"
            )
        object_key = ObjectTableConfig._extract_object_key_from_uri(destination_uri)
        payload = cls._serialize_cursor_for_object_store(cursor, format=format, compression=compression)
        put_fn = getattr(store, "put", None)
        if callable(put_fn):
            res = put_fn(object_key, payload)
            if inspect.isawaitable(res):
                await res
            return
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(store).__name__!r}")

    @classmethod
    def read_arrow_cursor_from_object_store_sync(
        cls,
        uri: str,
        *,
        store: Any | None,
        format: str,
    ) -> ArrowCursor:
        """Read an `ArrowCursor` from a multicloud `ObjectStore` or `ObjectStoreBackend`."""
        backend = cls._resolve_object_store_backend(store, uri)
        if backend is not None:
            object_key = ObjectTableConfig._extract_object_key_from_uri(uri)
            if hasattr(backend, "read_arrow_sync"):
                file_fmt = cls._normalize_backend_file_format(format)
                res = backend.read_arrow_sync(object_key, file_format=file_fmt)
                return cls._coerce_read_arrow_result(res, format=format)
        if store is None:
            raise ImproperConfigurationError(
                "Non-GCS load_from_object_store requires an ObjectStore instance or ObjectStoreBackend"
            )
        object_key = ObjectTableConfig._extract_object_key_from_uri(uri)
        get_fn = getattr(store, "get", None)
        if callable(get_fn):
            raw = bytes(get_fn(object_key))
            return cls._deserialize_bytes_from_object_store(raw, format=format)
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(store).__name__!r}")

    @classmethod
    async def read_arrow_cursor_from_object_store_async(
        cls,
        uri: str,
        *,
        store: Any | None,
        format: str,
    ) -> ArrowCursor:
        """Asynchronously read an `ArrowCursor` from a multicloud `ObjectStore` or `ObjectStoreBackend`."""
        backend = cls._resolve_object_store_backend(store, uri)
        if backend is not None:
            object_key = ObjectTableConfig._extract_object_key_from_uri(uri)
            if hasattr(backend, "read_arrow_async"):
                file_fmt = cls._normalize_backend_file_format(format)
                res = await backend.read_arrow_async(object_key, file_format=file_fmt)
                return cls._coerce_read_arrow_result(res, format=format)
        if store is None:
            raise ImproperConfigurationError(
                "Non-GCS load_from_object_store requires an ObjectStore instance or ObjectStoreBackend"
            )
        object_key = ObjectTableConfig._extract_object_key_from_uri(uri)
        get_fn = getattr(store, "get", None)
        if callable(get_fn):
            res = get_fn(object_key)
            if inspect.isawaitable(res):
                res = await res
            raw = bytes(res)
            return cls._deserialize_bytes_from_object_store(raw, format=format)
        raise ImproperConfigurationError(f"Unsupported ObjectStore {type(store).__name__!r}")

    @classmethod
    def list_objects_from_store_fallback_sync(
        cls,
        config: ObjectTableConfig,
        store: Any,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List objects directly from `store` when the Object Table catalog query is unavailable."""
        rows: list[dict[str, Any]] = []
        list_fn = getattr(store, "list", None)
        list_backend_fn = getattr(store, "list_objects_sync", None)
        for pattern in config.uris:
            scheme = urlparse(pattern).scheme or "gs"
            bucket, prefix = cls._extract_bucket_prefix_from_uri(pattern)
            if callable(list_fn):
                items = list_fn(prefix or None)
            elif callable(list_backend_fn):
                items = list_backend_fn(prefix=prefix)
            else:
                items = []
            for item in items:
                rows.append(cls._normalize_store_item_to_row(item, scheme=scheme, bucket=bucket))
                if limit is not None and len(rows) >= limit:
                    return rows
        return rows

    @classmethod
    async def list_objects_from_store_fallback_async(
        cls,
        config: ObjectTableConfig,
        store: Any,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously list objects directly from `store` when the Object Table catalog query is unavailable."""
        rows: list[dict[str, Any]] = []
        list_fn = getattr(store, "list", None)
        list_backend_async = getattr(store, "list_objects_async", None)
        list_backend_sync = getattr(store, "list_objects_sync", None)
        for pattern in config.uris:
            scheme = urlparse(pattern).scheme or "gs"
            bucket, prefix = cls._extract_bucket_prefix_from_uri(pattern)
            if callable(list_backend_async):
                items = await list_backend_async(prefix=prefix)
            elif callable(list_backend_sync):
                items = list_backend_sync(prefix=prefix)
            elif callable(list_fn):
                res = list_fn(prefix or None)
                if inspect.isawaitable(res):
                    res = await res
                items = res
            else:
                items = []
            for item in items:
                rows.append(cls._normalize_store_item_to_row(item, scheme=scheme, bucket=bucket))
                if limit is not None and len(rows) >= limit:
                    return rows
        return rows

    @classmethod
    def _encode_param_descriptor(
        cls,
        value: Any,
        *,
        name: str | None = None,
    ) -> dict[str, Any]:
        descriptor: dict[str, Any] = {}
        if name is not None:
            descriptor["name"] = name.lstrip("@")

        if value is None:
            descriptor["type"] = "NULL"
            descriptor["sql_type"] = "STRING"
            descriptor["value"] = None
            return descriptor

        if isinstance(value, dict) and "type" in value and ("value" in value or "values" in value or "fields" in value):
            merged = dict(value)
            if name is not None:
                merged["name"] = name.lstrip("@")
            return merged

        if isinstance(value, bool):
            descriptor["type"] = "BOOL"
            descriptor["value"] = value
            return descriptor

        if isinstance(value, int):
            descriptor["type"] = "INT64"
            descriptor["value"] = value
            return descriptor

        if isinstance(value, float):
            descriptor["type"] = "FLOAT64"
            descriptor["value"] = value
            return descriptor

        if isinstance(value, Decimal):
            descriptor["type"] = "NUMERIC"
            descriptor["value"] = str(value)
            return descriptor

        if isinstance(value, (bytes, bytearray, memoryview)):
            raw_bytes = bytes(value)
            descriptor["type"] = "BYTES"
            descriptor["value"] = base64.b64encode(raw_bytes).decode("ascii")
            return descriptor

        if isinstance(value, datetime):
            descriptor["type"] = "TIMESTAMP"
            descriptor["value"] = time_iso_convert(value)
            return descriptor

        if isinstance(value, date):
            descriptor["type"] = "DATE"
            descriptor["value"] = time_iso_convert(value)
            return descriptor

        if isinstance(value, time):
            descriptor["type"] = "TIME"
            descriptor["value"] = time_iso_convert(value)
            return descriptor

        if isinstance(value, str):
            descriptor["type"] = "STRING"
            descriptor["value"] = value
            return descriptor

        if isinstance(value, (list, tuple)):
            encoded_items = [cls._encode_param_descriptor(item) for item in value]
            elem_type = (
                str(encoded_items[0].get("type", "STRING"))
                if encoded_items and encoded_items[0].get("type") != "NULL"
                else "STRING"
            )
            descriptor["type"] = "ARRAY"
            descriptor["elem_type"] = elem_type
            descriptor["values"] = encoded_items
            return descriptor

        if isinstance(value, Mapping):
            fields_list: list[dict[str, Any]] = []
            for k, v in value.items():
                sub = cls._encode_param_descriptor(v)
                fields_list.append({"name": str(k), "value": sub})
            descriptor["type"] = "STRUCT"
            descriptor["fields"] = fields_list
            return descriptor

        descriptor["type"] = "STRING"
        descriptor["value"] = str(value)
        return descriptor

    @staticmethod
    def _sanitize_label_component(raw: str, *, allow_empty: bool) -> str:
        lowered = raw.strip().lower()
        cleaned_chars = [ch if ("a" <= ch <= "z" or "0" <= ch <= "9" or ch in {"_", "-"}) else "_" for ch in lowered]
        result = "".join(cleaned_chars)[:63]
        if not result and not allow_empty:
            return "l"
        if not allow_empty and not ("a" <= result[0] <= "z"):
            result = ("l_" + result)[:63]
        return result

    @staticmethod
    def _serialize_cursor_for_object_store(
        cursor: ArrowCursor,
        *,
        format: str,
        compression: str,
    ) -> bytes:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc"}:
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
            lines = [",".join(cols)]
            for r in rows:
                lines.append(",".join("" if r.get(c) is None else str(r.get(c)) for c in cols))
            return ("\n".join(lines) + "\n").encode("utf-8")
        rows = cursor.to_pylist()
        return ("\n".join(encode_json(r) for r in rows) + ("\n" if rows else "")).encode("utf-8")

    @staticmethod
    def _deserialize_bytes_from_object_store(
        payload: bytes,
        *,
        format: str,
    ) -> ArrowCursor:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc"}:
            return ArrowCursor(payload)
        if fmt == "parquet":
            pa = import_optional("pyarrow")
            pq = import_optional("pyarrow.parquet")
            if pa is not None and pq is not None:
                tbl = pq.read_table(pa.py_buffer(payload))
                ipc_view = write_arrow_shm(tbl)
                return ArrowCursor(bytes(ipc_view))
        if fmt in {"json", "ndjson"}:
            lines = [ln.strip() for ln in payload.decode("utf-8", errors="replace").splitlines() if ln.strip()]
            rows = [decode_json(ln) for ln in lines]
            return ArrowCursor.from_rows(rows, threshold_rows=1)
        return ArrowCursor(payload)

    @staticmethod
    def _resolve_object_store_backend(store: Any | None, uri: str) -> Any | None:
        storage_mod = import_optional("vertebra.extensions.sqlspec.storage")
        if storage_mod is None or not hasattr(storage_mod, "ObjectStoreBackend"):
            return None
        backend_cls = storage_mod.ObjectStoreBackend
        if isinstance(store, backend_cls):
            return store
        if isinstance(store, str):
            return backend_cls(store)
        if store is not None:
            return backend_cls(uri, store=store)
        if "://" in uri:
            return backend_cls(uri)
        return None

    @classmethod
    def _coerce_read_arrow_result(cls, res: Any, *, format: str) -> ArrowCursor:
        if isinstance(res, ArrowCursor):
            return res
        if isinstance(res, (bytes, bytearray, memoryview)):
            return cls._deserialize_bytes_from_object_store(bytes(res), format=format)
        return ArrowCursor(bytes(write_arrow_shm(res)))

    @staticmethod
    def _normalize_backend_file_format(format: str) -> str:
        fmt = format.strip().lower()
        if fmt in {"arrow", "ipc", "arrow-ipc"}:
            return "arrow-ipc"
        return fmt

    @staticmethod
    def _extract_bucket_prefix_from_uri(uri: str) -> tuple[str, str]:
        if "://" not in uri:
            return "", uri.lstrip("/").rstrip("*")
        parsed = urlparse(uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("*")
        return bucket, prefix

    @staticmethod
    def _normalize_store_item_to_row(
        item: Any,
        *,
        scheme: str,
        bucket: str,
    ) -> dict[str, Any]:
        if isinstance(item, str):
            loc = item.lstrip("/")
            full_uri = f"{scheme}://{bucket}/{loc}" if bucket else f"{scheme}://{loc}"
            return {
                "uri": full_uri,
                "generation": None,
                "content_type": None,
                "size": 0,
                "md5_hash": None,
                "updated": None,
            }
        if isinstance(item, Mapping):
            loc = str(item.get("location") or item.get("path") or "").lstrip("/")
            full_uri = f"{scheme}://{bucket}/{loc}" if bucket else loc
            return {
                "uri": full_uri,
                "generation": item.get("version") or item.get("e_tag") or item.get("etag"),
                "content_type": item.get("content_type"),
                "size": int(item.get("size", 0) or 0),
                "md5_hash": item.get("e_tag") or item.get("etag"),
                "updated": item.get("last_modified"),
            }
        loc = str(getattr(item, "path", None) or getattr(item, "location", "") or "").lstrip("/")
        full_uri = f"{scheme}://{bucket}/{loc}" if bucket else loc
        etag = getattr(item, "etag", None) or getattr(item, "e_tag", None)
        version = getattr(item, "version", None) or etag
        return {
            "uri": full_uri,
            "generation": version,
            "content_type": getattr(item, "content_type", None),
            "size": int(getattr(item, "size", 0) or 0),
            "md5_hash": etag,
            "updated": str(getattr(item, "last_modified", ""))
            if getattr(item, "last_modified", None) is not None
            else None,
        }
