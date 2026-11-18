"""Core protocol encoding, identifier parsing, and Arrow/ObjectStore helpers for BigQuery."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Final

import msgspec

from vertebra._vertebra import (
    BigQueryError,
    hydrate_rows_vectorcall,
)
from vertebra.db.adapters.bigquery.config import (
    BigQueryConfig,
    BigQueryDryRunResult,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor, write_arrow_shm
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.schema import can_vectorcall_hydrate_schema, to_schema
from vertebra.utils.serializers import encode_json
from vertebra.utils.text import split_qualified_identifier, validate_identifier
from vertebra.utils.type_converters import time_iso_convert

if TYPE_CHECKING:
    from vertebra._bigquery import (
        AsyncBigQueryClient as _RustAsyncBigQueryClient,
    )
    from vertebra._bigquery import (
        SyncBigQueryClient as _RustSyncBigQueryClient,
    )

__all__ = (
    "BigQueryCore",
    "BigQueryTableIdentifier",
)

_ON_DEMAND_TIB_PRICE_USD: Final[float] = 6.25
_BYTES_PER_TIB: Final[float] = float(1 << 40)
_STORAGE_API_FALLBACK_CODES: Final[frozenset[int]] = frozenset({7, 9, 12, 14})
_MAX_APPEND_ROWS_BATCH_BYTES: Final[int] = 9 * 1024 * 1024
_APPEND_ROWS_FRAME_HEADROOM_BYTES: Final[int] = 64 * 1024


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
        normalize_nested: bool = True,
    ) -> list[Any]:
        """Hydrate query rows into dictionaries or `schema_type` instances via C-API vectorcall or `to_schema`."""
        if not rows:
            return []
        col_tuple = tuple(columns)
        if schema_type is None or schema_type is dict:
            return [dict(zip(col_tuple, row, strict=False)) for row in rows]
        if can_vectorcall_hydrate_schema(schema_type):
            try:
                return hydrate_rows_vectorcall(schema_type, list(rows), col_tuple)
            except Exception:
                pass
        dicts = [dict(zip(col_tuple, row, strict=False)) for row in rows]
        try:
            return list(to_schema(dicts, schema_type=schema_type, normalize_nested=normalize_nested))
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
        return BigQueryTableIdentifier.split_parts(raw)

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
    def query_result_to_arrow_cursor(
        raw: Mapping[str, Any],
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Convert a REST `jobs.query` response dictionary into an `ArrowCursor`."""
        columns = [str(c) for c in raw.get("columns", ())]
        rows = list(raw.get("rows", ()))
        return ArrowCursor.from_rows(rows, columns=columns, threshold_rows=threshold_rows)

    @classmethod
    def coerce_arrow_ipc_bytes(cls, data: ArrowCursor | bytes | Any) -> bytes:
        """Extract serialized Arrow IPC stream bytes, casting dictionaries and splitting >9 MiB batches."""
        if isinstance(data, bytes):
            return data
        if isinstance(data, ArrowCursor):
            if not data.used_shm and isinstance(data._buffer, bytes):
                return data._buffer
            return bytes(data.buffer_view())
        if isinstance(data, (bytearray, memoryview)):
            return bytes(data)
        pa = import_optional("pyarrow")
        if pa is not None and hasattr(data, "schema"):
            return cls._serialize_pyarrow_for_append(pa, data)
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

    @classmethod
    def validate_and_quote_table(cls, table: str, *, field_name: str = "table") -> str:
        """Validate a BigQuery table reference and return a canonical backtick-quoted SQL identifier."""
        return BigQueryTableIdentifier.validate_and_quote(table, field_name=field_name)

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

    @classmethod
    def _serialize_pyarrow_for_append(cls, pa: Any, data: Any) -> bytes:
        if any(pa.types.is_dictionary(field.type) for field in data.schema):
            clean_schema = pa.schema(
                [
                    field.with_type(field.type.value_type) if pa.types.is_dictionary(field.type) else field
                    for field in data.schema
                ]
            )
            data = (
                data.cast(clean_schema) if hasattr(data, "cast") else pa.Table.from_batches([data]).cast(clean_schema)
            )
        schema = data.schema
        raw_batches = data.to_batches() if hasattr(data, "to_batches") else [data]
        safe_limit = _MAX_APPEND_ROWS_BATCH_BYTES - _APPEND_ROWS_FRAME_HEADROOM_BYTES
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            for batch in raw_batches:
                if batch.num_rows > 1 and batch.nbytes > safe_limit:
                    for sub_batch in cls._split_oversized_arrow_batch(pa, batch, schema):
                        writer.write_batch(sub_batch)
                else:
                    writer.write_batch(batch)
        ipc_bytes: bytes = sink.getvalue().to_pybytes()
        return ipc_bytes

    @classmethod
    def _split_oversized_arrow_batch(cls, pa: Any, batch: Any, schema: Any) -> list[Any]:
        safe_limit = _MAX_APPEND_ROWS_BATCH_BYTES - _APPEND_ROWS_FRAME_HEADROOM_BYTES
        chunk_rows = max(1, (batch.num_rows * safe_limit) // max(1, int(batch.nbytes)))
        chunks: list[Any] = []
        offset = 0
        while offset < batch.num_rows:
            step = min(chunk_rows, batch.num_rows - offset)
            candidate = batch.slice(offset, step)
            while step > 1 and cls._serialized_batch_stream_size(pa, candidate, schema) > _MAX_APPEND_ROWS_BATCH_BYTES:
                step = max(1, step // 2)
                candidate = batch.slice(offset, step)
            chunks.append(candidate)
            offset += step
        return chunks

    @staticmethod
    def _serialized_batch_stream_size(pa: Any, batch: Any, schema: Any) -> int:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(batch)
        return int(sink.getvalue().size)


class BigQueryTableIdentifier:
    """Canonical BigQuery table identifier parser, validator, and GoogleSQL backtick quoter."""

    @staticmethod
    def split_parts(raw: str) -> list[str]:
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
    def parse_parts(cls, table: str, *, field_name: str = "table") -> tuple[str, ...]:
        """Validate a BigQuery table reference and return its normalized unquoted segments."""
        stripped = table.strip()
        if not stripped or any(ch.isspace() or ch in {";", "(", ")", "'", '"'} for ch in stripped):
            msg = f"Invalid BigQuery identifier for {field_name}: {table!r}"
            raise ValueError(msg)
        if "--" in stripped or "/*" in stripped or "*/" in stripped:
            msg = f"Invalid BigQuery identifier for {field_name}: {table!r}"
            raise ValueError(msg)

        if stripped.startswith("projects/"):
            if "`" in stripped:
                msg = f"Invalid BigQuery identifier for {field_name}: {table!r}"
                raise ValueError(msg)
            segments = stripped.split("/")
            if len(segments) != 6 or segments[0] != "projects" or segments[2] != "datasets" or segments[4] != "tables":
                msg = f"Invalid BigQuery resource identifier for {field_name}: {table!r}"
                raise ValueError(msg)
            cls._validate_project_id(segments[1], field_name=field_name)
            validate_identifier(segments[3], field_name, allow_dots=False, allow_hyphens=False)
            validate_identifier(segments[5], field_name, allow_dots=False, allow_hyphens=True)
            return (segments[1], segments[3], segments[5])

        if "`" in stripped:
            if stripped.startswith("`") and stripped.endswith("`") and "`" not in stripped[1:-1]:
                parts = cls.split_parts(stripped[1:-1])
            else:
                parts = cls._split_backticked_segments(stripped, original=table, field_name=field_name)
        else:
            parts = cls.split_parts(stripped)

        if len(parts) == 3:
            cls._validate_project_id(parts[0], field_name=field_name)
            validate_identifier(parts[1], field_name, allow_dots=False, allow_hyphens=False)
            validate_identifier(parts[2], field_name, allow_dots=False, allow_hyphens=True)
            return (parts[0], parts[1], parts[2])
        if len(parts) == 2:
            validate_identifier(parts[0], field_name, allow_dots=False, allow_hyphens=False)
            validate_identifier(parts[1], field_name, allow_dots=False, allow_hyphens=True)
            return (parts[0], parts[1])
        if len(parts) == 1:
            validate_identifier(parts[0], field_name, allow_dots=False, allow_hyphens=True)
            return (parts[0],)
        msg = f"Invalid BigQuery identifier for {field_name}: {table!r}"
        raise ValueError(msg)

    @classmethod
    def validate_and_quote(cls, table: str, *, field_name: str = "table") -> str:
        """Validate a BigQuery table reference and return canonical per-segment backtick-quoted SQL."""
        parts = cls.parse_parts(table, field_name=field_name)
        return ".".join(f"`{part}`" for part in parts)

    @staticmethod
    def _validate_project_id(project_id: str, *, field_name: str) -> None:
        if ":" in project_id:
            if project_id.count(":") != 1:
                msg = f"Invalid BigQuery project identifier for {field_name}: {project_id!r}"
                raise ValueError(msg)
            domain, proj_slug = project_id.split(":", 1)
            validate_identifier(domain, field_name, allow_dots=True, allow_hyphens=True)
            validate_identifier(proj_slug, field_name, allow_dots=False, allow_hyphens=True)
            return
        validate_identifier(project_id, field_name, allow_dots=False, allow_hyphens=True)

    @staticmethod
    def _split_backticked_segments(stripped: str, *, original: str, field_name: str) -> list[str]:
        segments: list[str] = []
        idx = 0
        n = len(stripped)
        first_bt = stripped.find("`")
        colon = stripped.find(":")
        if colon != -1 and (first_bt == -1 or colon < first_bt):
            prefix = stripped[:colon]
            rest = stripped[colon + 1 :]
            if "." in prefix:
                dot_after = rest.find(".")
                if dot_after == -1 or dot_after + 1 >= len(rest):
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                proj_part = f"{prefix}:{rest[:dot_after]}"
                if "`" in proj_part:
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                segments.append(proj_part)
                stripped = rest[dot_after + 1 :]
            else:
                if not prefix or "`" in prefix or not rest:
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                segments.append(prefix)
                stripped = rest
            n = len(stripped)

        while idx < n:
            if stripped[idx] == "`":
                end = stripped.find("`", idx + 1)
                if end == -1:
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                segment = stripped[idx + 1 : end]
                if not segment or "`" in segment:
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                segments.append(segment)
                idx = end + 1
                if idx < n:
                    if stripped[idx] != "." or idx + 1 >= n:
                        msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                        raise ValueError(msg)
                    idx += 1
            else:
                dot = stripped.find(".", idx)
                end = n if dot == -1 else dot
                segment = stripped[idx:end]
                if not segment or "`" in segment:
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                segments.append(segment)
                if dot == -1:
                    break
                if dot + 1 >= n:
                    msg = f"Invalid BigQuery identifier for {field_name}: {original!r}"
                    raise ValueError(msg)
                idx = dot + 1
        return segments
