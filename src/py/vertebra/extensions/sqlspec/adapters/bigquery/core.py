"""BigQuery SQLSpec core statement compilation, parameter codecs, connection shim, and error mapping."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Final, Self, cast
from urllib.parse import urlparse

from sqlspec.core import (
    SQL,
    DriverParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    StatementConfig,
    TypedParameter,
    build_statement_config_from_profile,
)
from sqlspec.exceptions import (
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DialectNotSupportedError,
    IntegrityError,
    NotFoundError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_uuid_coercions

from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    DatabaseError,
    GoogleCloudAuthError,
    GoogleCloudError,
    NotSupportedError,
)
from vertebra.db.adapters.bigquery import BigQueryConfig, BigQueryCore, ObjectRef
from vertebra.db.arrow import ArrowCursor
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.storage import ArtifactReference
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "BIGQUERY_DEFAULT_SCOPES",
    "BIGQUERY_STATEMENT_CONFIG",
    "BigQueryConnectionQueryJob",
    "BigQueryConnectionShim",
    "bigquery_partition_clauses",
    "build_profile",
    "build_statement_config",
    "compile_bigquery_statement",
    "default_statement_config",
    "driver_profile",
    "encode_typed_bigquery_many",
    "encode_typed_bigquery_parameters",
    "extract_bigquery_job_config",
    "is_bigquery_table_missing",
    "is_compound_googlesql_script",
    "map_bigquery_exception",
    "object_ref_to_artifact_reference",
    "parse_utc_timestamp",
    "positive_int_or_none",
    "qualify_store_table",
    "quote_bigquery_identifier",
    "quote_bigquery_table",
    "resolve_bigquery_config",
    "resolve_dataset_id",
    "resolve_project_id",
    "resolve_storage_backend",
    "utc_now_iso",
)

BIGQUERY_DEFAULT_SCOPES: Final[tuple[str, ...]] = ("https://www.googleapis.com/auth/bigquery",)

_LINE_COMMENT_RE: Final[re.Pattern[str]] = re.compile(r"--[^\r\n]*")
_HASH_COMMENT_RE: Final[re.Pattern[str]] = re.compile(r"#[^\r\n]*")
_BLOCK_COMMENT_RE: Final[re.Pattern[str]] = re.compile(r"/\*.*?\*/", re.DOTALL)
_SINGLE_QUOTE_RE: Final[re.Pattern[str]] = re.compile(r"'(?:\\.|[^'\\])*'")
_DOUBLE_QUOTE_RE: Final[re.Pattern[str]] = re.compile(r'"(?:\\.|[^"\\])*"')
_BACKTICK_RE: Final[re.Pattern[str]] = re.compile(r"`[^`]*`")
_COMPOUND_START_RE: Final[re.Pattern[str]] = re.compile(
    r"^\s*(?:DECLARE\b|BEGIN\b|LOOP\b|WHILE\b|REPEAT\b|FOR\b|IF\b|CASE\b|"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+|TEMPORARY\s+)?(?:PROCEDURE|FUNCTION)\b)",
    re.IGNORECASE,
)
_COMPOUND_BODY_RE: Final[re.Pattern[str]] = re.compile(
    r"\bBEGIN\b.*\bEND\b|\bDECLARE\b|\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\b",
    re.IGNORECASE | re.DOTALL,
)
_HTTP_STATUS_RE: Final[re.Pattern[str]] = re.compile(r"\b(?:HTTP\s+)?(400|401|403|404|408|409|429|500|502|503|504)\b")
_IDENT_PART_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_$-]*$")
_BIGQUERY_ALLOWED_SCHEMES: Final[frozenset[str]] = frozenset({"bigquery", "bq"})


def driver_profile() -> DriverParameterProfile:
    """Return the BigQuery ``@name`` SQLSpec parameter profile."""
    return DriverParameterProfile(
        name="BigQuery",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={
            ParameterStyle.NAMED_AT,
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_COLON,
        },
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=True,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        custom_type_coercions=build_uuid_coercions(),
        default_dialect="bigquery",
        extras={"preserve_typed_parameters": True},
        strict_named_parameters=False,
    )


def build_profile() -> DriverParameterProfile:
    """Return a fresh copy of the BigQuery driver parameter profile."""
    return driver_profile()


def build_statement_config(
    *,
    json_serializer: Callable[[Any], str] = to_json,
    json_deserializer: Callable[[str], Any] = from_json,
) -> StatementConfig:
    """Construct a BigQuery ``StatementConfig`` with ``@name`` execution binding."""
    config = build_statement_config_from_profile(
        driver_profile(),
        json_serializer=json_serializer,
        json_deserializer=json_deserializer,
    )
    return config.replace(
        dialect="bigquery",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NAMED_AT,
            supported_parameter_styles={
                ParameterStyle.NAMED_AT,
                ParameterStyle.QMARK,
                ParameterStyle.NUMERIC,
                ParameterStyle.NAMED_COLON,
            },
            default_execution_parameter_style=ParameterStyle.NAMED_AT,
            supported_execution_parameter_styles={ParameterStyle.NAMED_AT},
            preserve_parameter_format=True,
            needs_static_script_compilation=False,
            allow_mixed_parameter_styles=True,
            strict_named_parameters=False,
        ),
    )


def default_statement_config() -> StatementConfig:
    """Return the canonical BigQuery SQLSpec ``StatementConfig``."""
    return build_statement_config()


BIGQUERY_STATEMENT_CONFIG: Final[StatementConfig] = default_statement_config()


def _infer_bigquery_type_name(value: Any, semantic_name: str | None = None, original_type: Any = None) -> str:
    """Infer an explicit GoogleSQL parameter type name from semantic hints or Python values."""
    if semantic_name:
        return semantic_name.strip().upper()
    if original_type is dict or isinstance(value, Mapping):
        return "JSON"
    if isinstance(value, bool) or original_type is bool:
        return "BOOL"
    if isinstance(value, int) or original_type is int:
        return "INT64"
    if isinstance(value, float) or original_type is float:
        return "FLOAT64"
    if isinstance(value, Decimal) or original_type is Decimal:
        return "NUMERIC"
    if isinstance(value, (bytes, bytearray, memoryview)) or original_type in (bytes, bytearray):
        return "BYTES"
    if isinstance(value, datetime) or original_type is datetime:
        return "TIMESTAMP"
    if isinstance(value, date) or original_type is date:
        return "DATE"
    if isinstance(value, time) or original_type is time:
        return "TIME"
    if isinstance(value, (list, tuple)):
        if not value:
            return "ARRAY<STRING>"
        elem_type = _infer_bigquery_type_name(value[0])
        return f"ARRAY<{elem_type}>"
    return "STRING"


def _encode_single_typed_value(param: TypedParameter) -> dict[str, Any]:
    """Convert a ``TypedParameter`` into a BigQuery parameter descriptor dict."""
    bq_type = _infer_bigquery_type_name(param.value, param.semantic_name, param.original_type)
    raw = param.value
    if bq_type == "STRUCT" or bq_type.startswith("STRUCT<"):
        if isinstance(raw, Mapping):
            fields: dict[str, Any] = {}
            for key, item in raw.items():
                if isinstance(item, TypedParameter):
                    fields[str(key)] = _encode_single_typed_value(item)
                else:
                    fields[str(key)] = {"type": _infer_bigquery_type_name(item), "value": item}
            return {"type": bq_type, "fields": fields}
        return {"type": bq_type, "value": raw}
    if bq_type.startswith("ARRAY<"):
        seq = list(raw) if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)) else [raw]
        values = [_encode_single_typed_value(item) if isinstance(item, TypedParameter) else item for item in seq]
        return {"type": bq_type, "values": values}
    if bq_type == "JSON" and not isinstance(raw, str):
        return {"type": "JSON", "value": to_json(raw)}
    if isinstance(raw, memoryview):
        return {"type": bq_type, "value": raw.tobytes()}
    return {"type": bq_type, "value": raw}


def _encode_parameter_item(value: Any) -> Any:
    """Preserve BigQuery ``TypedParameter`` descriptors while leaving scalars intact."""
    if isinstance(value, TypedParameter):
        return _encode_single_typed_value(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return value


def encode_typed_bigquery_parameters(parameters: Any) -> Any:
    """Normalize compiled SQLSpec parameters into BigQuery-compatible ``@name`` binds."""
    if parameters is None:
        return None
    if isinstance(parameters, Mapping):
        return {str(key): _encode_parameter_item(val) for key, val in parameters.items()}
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        return [_encode_parameter_item(item) for item in parameters]
    return parameters


def encode_typed_bigquery_many(parameter_sets: Any) -> list[Any]:
    """Encode batch parameter sets for BigQuery executemany execution."""
    if parameter_sets is None:
        return []
    if isinstance(parameter_sets, Sequence) and not isinstance(parameter_sets, (str, bytes, bytearray)):
        return [encode_typed_bigquery_parameters(item) for item in parameter_sets]
    return [encode_typed_bigquery_parameters(parameter_sets)]


def compile_bigquery_statement(statement: SQL) -> tuple[str, Any]:
    """Compile a SQLSpec ``SQL`` statement once and encode BigQuery typed parameters."""
    sql, prepared = statement.compile()
    if prepared is None:
        return sql, None
    if isinstance(prepared, (dict, list, tuple)) and len(prepared) == 0:
        return sql, None
    return sql, encode_typed_bigquery_parameters(prepared)


def _strip_sql_noise(sql: str) -> str:
    """Remove comments and quoted literals so procedural BigQuery block detection stays exact."""
    cleaned = _BLOCK_COMMENT_RE.sub(" ", sql)
    cleaned = _LINE_COMMENT_RE.sub(" ", cleaned)
    cleaned = _HASH_COMMENT_RE.sub(" ", cleaned)
    cleaned = _SINGLE_QUOTE_RE.sub("''", cleaned)
    cleaned = _DOUBLE_QUOTE_RE.sub('""', cleaned)
    return _BACKTICK_RE.sub("``", cleaned)


def is_compound_googlesql_script(sql: str) -> bool:
    """Return True when ``sql`` contains procedural GoogleSQL blocks requiring unsplit execution."""
    stripped = _strip_sql_noise(sql).strip()
    if not stripped:
        return False
    return bool(_COMPOUND_START_RE.match(stripped) or _COMPOUND_BODY_RE.search(stripped))


def positive_int_or_none(value: Any, key: str) -> int | None:
    """Validate optional positive integer configuration values cleanly."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"BigQuery option '{key}' must be a positive integer"
        raise ImproperConfigurationError(msg)
    return value


def quote_bigquery_identifier(identifier: str) -> str:
    """Backtick-quote a single BigQuery identifier safely."""
    stripped = identifier.strip().strip("`")
    if not stripped:
        msg = "BigQuery identifier must not be empty"
        raise ImproperConfigurationError(msg)
    escaped = stripped.replace("`", "\\`")
    return f"`{escaped}`"


def quote_bigquery_table(table_name: str) -> str:
    """Validate and backtick-quote a 1-, 2-, or 3-part BigQuery table path."""
    if hasattr(BigQueryCore, "validate_and_quote_table"):
        return str(BigQueryCore.validate_and_quote_table(table_name))
    parts = [part.strip().strip("`") for part in table_name.split(".") if part.strip().strip("`")]
    if not parts or len(parts) > 3:
        msg = f"Invalid BigQuery table identifier: {table_name!r}"
        raise ImproperConfigurationError(msg)
    for part in parts:
        if not _IDENT_PART_RE.match(part):
            msg = f"Invalid BigQuery identifier segment: {part!r}"
            raise ImproperConfigurationError(msg)
    return ".".join(f"`{part}`" for part in parts)


def _extract_from_object_or_mapping(source: Any, attr: str) -> str | None:
    """Read an optional string attribute from a driver, shim, config, or mapping."""
    if source is None:
        return None
    if isinstance(source, Mapping):
        val = source.get(attr)
        return str(val) if isinstance(val, str) and val.strip() else None
    val = getattr(source, attr, None)
    if isinstance(val, str) and val.strip():
        return val.strip()
    cfg = getattr(source, "config", None)
    if cfg is not None and cfg is not source:
        nested = getattr(cfg, attr, None)
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
        if hasattr(cfg, f"effective_{attr}"):
            effective = getattr(cfg, f"effective_{attr}")
            if isinstance(effective, str) and effective.strip():
                return effective.strip()
    conn_cfg = getattr(source, "connection_config", None)
    if isinstance(conn_cfg, Mapping):
        mapped = conn_cfg.get(attr)
        if isinstance(mapped, str) and mapped.strip():
            return mapped.strip()
    conn_inst = getattr(source, "connection_instance", None)
    if conn_inst is not None and conn_inst is not source:
        return _extract_from_object_or_mapping(conn_inst, attr)
    return None


def resolve_project_id(driver_or_config: Any) -> str | None:
    """Resolve the active BigQuery project ID from a driver, shim, or config."""
    return _extract_from_object_or_mapping(driver_or_config, "project_id") or _extract_from_object_or_mapping(
        driver_or_config, "project"
    )


def resolve_dataset_id(driver_or_config: Any) -> str | None:
    """Resolve the default BigQuery dataset ID from a driver, shim, or config."""
    return _extract_from_object_or_mapping(driver_or_config, "dataset_id") or _extract_from_object_or_mapping(
        driver_or_config, "dataset"
    )


def qualify_store_table(driver_or_config: Any, table_name: str) -> str:
    """Qualify an ADK, Events, or Litestar store table name with project and dataset when present."""
    stripped = table_name.strip().replace("`", "")
    parts = [part.strip() for part in stripped.split(".") if part.strip()]
    if len(parts) >= 2:
        return quote_bigquery_table(".".join(parts))
    project_id = resolve_project_id(driver_or_config)
    dataset_id = resolve_dataset_id(driver_or_config)
    if project_id and dataset_id:
        return quote_bigquery_table(f"{project_id}.{dataset_id}.{stripped}")
    if dataset_id:
        return quote_bigquery_table(f"{dataset_id}.{stripped}")
    return quote_bigquery_table(stripped)


def bigquery_partition_clauses(
    column: str,
    *,
    enabled: bool = True,
    expiration_days: int | None = None,
    require_filter: bool = False,
    cluster_by: Sequence[str] = (),
) -> tuple[str, str]:
    """Return `(partition_clause, options_clause)` suffixes for BigQuery ``CREATE TABLE`` DDL."""
    if not enabled:
        cluster_clause = f"\nCLUSTER BY {', '.join(cluster_by)}" if cluster_by else ""
        return cluster_clause, ""
    clauses = [f"\nPARTITION BY DATE({column})"]
    if cluster_by:
        clauses.append(f"\nCLUSTER BY {', '.join(cluster_by)}")
    options: list[str] = []
    if expiration_days is not None:
        valid_days = positive_int_or_none(expiration_days, "partition_expiration_days")
        if valid_days is not None:
            options.append(f"partition_expiration_days = {valid_days}")
    if require_filter:
        options.append("require_partition_filter = TRUE")
    options_sql = f"\nOPTIONS({', '.join(options)})" if options else ""
    return "".join(clauses), options_sql


def resolve_bigquery_config(
    connection_config: BigQueryConfig | Mapping[str, Any] | str | None = None,
    *,
    dsn: str | None = None,
    project_id: str | None = None,
    dataset_id: str | None = None,
    location: str = "US",
    credentials_path: str | None = None,
    scopes: Sequence[str] | None = None,
    session_enabled: bool = False,
    maximum_bytes_billed: int | None = None,
    rest_endpoint: str | None = None,
    storage_endpoint: str | None = None,
    **extra_kwargs: Any,
) -> BigQueryConfig:
    """Resolve keyword, DSN, or mapping parameters into a validated ``BigQueryConfig``."""
    _ = session_enabled
    if isinstance(connection_config, BigQueryConfig):
        return connection_config
    effective_dsn = dsn or (connection_config if isinstance(connection_config, str) else None)
    config_map: dict[str, Any] = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    config_map.update({k: v for k, v in extra_kwargs.items() if v is not None})
    if effective_dsn is None:
        raw_dsn = config_map.get("dsn") or config_map.get("url")
        if isinstance(raw_dsn, str) and raw_dsn.strip():
            effective_dsn = raw_dsn
    if effective_dsn is not None:
        parsed = urlparse(effective_dsn)
        scheme = parsed.scheme.lower()
        if scheme not in _BIGQUERY_ALLOWED_SCHEMES:
            msg = f"BigQuery DSN must use bigquery:// or bq:// scheme, got {parsed.scheme!r}"
            raise ImproperConfigurationError(msg)
        return BigQueryConfig.from_dsn(effective_dsn)

    effective_project = str(project_id or config_map.get("project_id") or config_map.get("project") or "")
    effective_dataset = dataset_id or config_map.get("dataset_id") or config_map.get("dataset")
    effective_location = str(config_map.get("location") or location)
    effective_creds = credentials_path or config_map.get("credentials_path")
    effective_scopes = tuple(scopes or config_map.get("scopes") or BIGQUERY_DEFAULT_SCOPES)
    effective_max_bytes = config_map.get("maximum_bytes_billed", maximum_bytes_billed)
    effective_rest = rest_endpoint or config_map.get("rest_endpoint")
    effective_storage = storage_endpoint or config_map.get("storage_endpoint")
    kwargs: dict[str, Any] = {
        "project_id": effective_project,
        "dataset_id": effective_dataset,
        "location": effective_location,
        "credentials_path": effective_creds,
        "scopes": effective_scopes,
        "maximum_bytes_billed": effective_max_bytes,
    }
    for optional_key in (
        "job_timeout_ms",
        "priority",
        "labels",
        "service_account_key",
        "quota_project_id",
        "impersonate_service_account",
        "access_token",
        "use_gcloud_cli",
        "pool_size",
        "statement_cache_capacity",
        "use_shm_cache",
        "arrow_compression",
        "max_read_streams",
        "ca_pem",
    ):
        if optional_key in config_map and config_map[optional_key] is not None:
            kwargs[optional_key] = config_map[optional_key]
    if "statement_cache_size" in config_map and config_map["statement_cache_size"] is not None:
        kwargs.setdefault("statement_cache_capacity", int(config_map["statement_cache_size"]))
    if effective_rest is not None:
        kwargs["rest_endpoint"] = effective_rest
    if effective_storage is not None:
        kwargs["storage_endpoint"] = effective_storage
    return BigQueryConfig(**kwargs)


def resolve_storage_backend(
    storage_backend: ObjectStoreBackend | str | None = None,
    driver_features: Mapping[str, Any] | None = None,
    *,
    artifact_storage_uri: str | None = None,
) -> ObjectStoreBackend | None:
    """Resolve an ``ObjectStoreBackend`` instance from explicit arguments, features, or URI."""
    candidate: Any = storage_backend
    if candidate is None and driver_features is not None:
        candidate = driver_features.get("storage_backend")
    if candidate is None and artifact_storage_uri:
        candidate = artifact_storage_uri
    if candidate is None:
        return None
    if isinstance(candidate, ObjectStoreBackend):
        return candidate
    if isinstance(candidate, str):
        return ObjectStoreBackend(candidate)
    if hasattr(candidate, "read_bytes_sync") or hasattr(candidate, "write_bytes_sync"):
        return cast(ObjectStoreBackend, candidate)
    msg = f"Unsupported BigQuery storage backend configuration: {candidate!r}"
    raise ImproperConfigurationError(msg)


def parse_utc_timestamp(value: Any) -> datetime:
    """Normalize BigQuery ISO-8601 strings, epoch values, or datetimes to timezone-aware UTC."""
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    msg = f"Unsupported BigQuery timestamp value: {value!r}"
    raise DataError(msg)


def utc_now_iso() -> str:
    """Return the current UTC instant formatted as ISO-8601."""
    return datetime.now(UTC).isoformat()


def object_ref_to_artifact_reference(ref: ObjectRef) -> ArtifactReference:
    """Translate a native BigQuery ``ObjectRef`` into a storage-neutral ``ArtifactReference``."""
    raw_version = ref.version
    if raw_version is None:
        version_int = 1
    elif str(raw_version).isdigit():
        version_int = int(raw_version)
    else:
        version_int = 1
    updated_dt = parse_utc_timestamp(ref.updated) if ref.updated is not None else None
    return ArtifactReference(
        uri=ref.uri,
        version=version_int,
        content_type=ref.content_type,
        size=ref.size,
        updated=updated_dt,
        signed_url=ref.signed_url,
    )


def _encode_query_parameter(param: Any) -> tuple[str | None, Any]:
    """Decode a ``google.cloud.bigquery``-style query parameter or dict into BigQuery descriptor syntax."""
    if isinstance(param, TypedParameter):
        return None, _encode_single_typed_value(param)
    name = getattr(param, "name", None)
    param_name = str(name) if isinstance(name, str) and name else None
    if hasattr(param, "sub_parameters") and getattr(param, "sub_parameters", None) is not None:
        sub_params = param.sub_parameters
        fields: dict[str, Any] = {}
        if isinstance(sub_params, Mapping):
            for k, item in sub_params.items():
                _, encoded = _encode_query_parameter(item)
                fields[str(k)] = encoded
        elif isinstance(sub_params, Sequence):
            for idx, item in enumerate(sub_params):
                sub_name, encoded = _encode_query_parameter(item)
                fields[sub_name or f"field_{idx}"] = encoded
        return param_name, {"type": "STRUCT", "fields": fields}
    if hasattr(param, "values") and hasattr(param, "array_type"):
        array_type = str(param.array_type or "STRING").upper()
        values = list(param.values or ())
        return param_name, {"type": f"ARRAY<{array_type}>", "values": values}
    if hasattr(param, "value") or hasattr(param, "type_"):
        value = getattr(param, "value", None)
        type_name = getattr(param, "type_", None)
        if type_name:
            return param_name, {"type": str(type_name).upper(), "value": value}
        return param_name, value
    return param_name, param


def extract_bigquery_job_config(
    parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    job_config: Any = None,
) -> tuple[Mapping[str, Any] | Sequence[Any] | None, dict[str, Any]]:
    """Extract normalized parameters and job options from a BigQuery ``QueryJobConfig``-like object."""
    merged_params: Any = parameters
    job_kwargs: dict[str, Any] = {}
    if job_config is None:
        return encode_typed_bigquery_parameters(merged_params), job_kwargs

    query_parameters = getattr(job_config, "query_parameters", None)
    if query_parameters:
        named: dict[str, Any] = dict(merged_params) if isinstance(merged_params, Mapping) else {}
        positional: list[Any] = (
            list(merged_params)
            if isinstance(merged_params, Sequence) and not isinstance(merged_params, (str, bytes, bytearray))
            else []
        )
        for item in query_parameters:
            param_name, encoded = _encode_query_parameter(item)
            if param_name:
                named[param_name] = encoded
            else:
                positional.append(encoded)
        merged_params = named if named else positional

    max_bytes = getattr(job_config, "maximum_bytes_billed", None)
    if max_bytes is not None:
        job_kwargs["maximum_bytes_billed"] = max_bytes
    labels = getattr(job_config, "labels", None)
    if labels:
        job_kwargs["job_labels"] = dict(labels)
    destination = getattr(job_config, "destination", None)
    if destination is not None:
        job_kwargs["destination_table"] = str(destination)
    write_disposition = getattr(job_config, "write_disposition", None)
    if write_disposition is not None:
        job_kwargs["write_disposition"] = str(write_disposition)
    return encode_typed_bigquery_parameters(merged_params), job_kwargs


def is_bigquery_table_missing(exc: BaseException) -> bool:
    """Return True when ``exc`` signals a missing BigQuery table or dataset."""
    if isinstance(exc, NotFoundError):
        return True
    message = str(exc).lower()
    return (
        "not found" in message
        or "does not exist" in message
        or "404" in message
        or "was not found in location" in message
    )


def map_bigquery_exception(exc: BaseException) -> SQLSpecError | None:
    """Map Vertebra native BigQuery, Google Cloud, and HTTP status errors to ``sqlspec.exceptions.*``."""
    if isinstance(exc, SQLSpecError):
        return exc
    message = str(exc)
    lower = message.lower()

    if isinstance(exc, NotSupportedError):
        return DialectNotSupportedError(message)
    if isinstance(exc, GoogleCloudAuthError):
        return PermissionDeniedError(message)
    if isinstance(exc, BigQueryRowError):
        return DataError(message)

    status_code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if not isinstance(status_code, int):
        matched = _HTTP_STATUS_RE.search(message)
        status_code = int(matched.group(1)) if matched else None

    if status_code in {401, 403}:
        return PermissionDeniedError(message)
    if status_code == 404:
        return NotFoundError(message)
    if status_code == 408 or "timeout" in lower or "timed out" in lower or "deadline exceeded" in lower:
        if status_code == 504 or "query" in lower or "deadline" in lower:
            return QueryTimeoutError(message)
        return ConnectionTimeoutError(message)
    if status_code == 409 or "already exists" in lower or "duplicate" in lower:
        return IntegrityError(message)
    if status_code in {429, 500, 502, 503, 504}:
        return OperationalError(message)
    if status_code == 400:
        if "syntax" in lower or "parse" in lower or "unrecognized name" in lower or "column" in lower:
            return SQLParsingError(message)
        return DataError(message)

    if isinstance(exc, (BigQueryError, GoogleCloudError, DatabaseError)):
        if "permission" in lower or "unauthorized" in lower or "access denied" in lower or "forbidden" in lower:
            return PermissionDeniedError(message)
        if "not found" in lower or "does not exist" in lower:
            return NotFoundError(message)
        if "syntax" in lower or "unrecognized" in lower or "invalid query" in lower:
            return SQLParsingError(message)
        if "connect" in lower or "network" in lower or "dns" in lower:
            return DatabaseConnectionError(message)
        return OperationalError(message)
    return None


class BigQueryConnectionQueryJob:
    """Lightweight query job handle returned by ``BigQueryConnectionShim.query()``."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        metadata: Mapping[str, Any],
        arrow_cursor: ArrowCursor | None = None,
    ) -> None:
        self._rows = rows
        self._metadata = dict(metadata)
        self._arrow_cursor = arrow_cursor

    @property
    def job_id(self) -> str | None:
        """Return the BigQuery job identifier."""
        val = self._metadata.get("job_id")
        return str(val) if val is not None else None

    @property
    def session_id(self) -> str | None:
        """Return the active BigQuery session identifier, if any."""
        val = self._metadata.get("session_id")
        return str(val) if val is not None else None

    @property
    def total_bytes_processed(self) -> int:
        """Return total bytes processed by the query job."""
        return int(self._metadata.get("total_bytes_processed", 0) or 0)

    @property
    def total_bytes_billed(self) -> int:
        """Return total bytes billed by the query job."""
        return int(self._metadata.get("total_bytes_billed", 0) or 0)

    @property
    def cache_hit(self) -> bool:
        """Return whether the query result was served from BigQuery cache."""
        return bool(self._metadata.get("cache_hit", False))

    @property
    def num_dml_affected_rows(self) -> int | None:
        """Return affected row count for DML statements."""
        val = self._metadata.get("num_dml_affected_rows")
        return int(val) if val is not None else None

    def result(self) -> list[dict[str, Any]]:
        """Return materialized query rows as dictionaries."""
        return list(self._rows)

    def to_arrow(self) -> Any:
        """Return the query result as an Arrow table."""
        if self._arrow_cursor is not None:
            return self._arrow_cursor.to_arrow_table()
        cursor = ArrowCursor.from_rows(self._rows, threshold_rows=1)
        return cursor.to_arrow_table()

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)


class BigQueryConnectionShim:
    """Zero-SDK connection shim exposing ``.query()`` with ``job_config`` parameter forwarding."""

    def __init__(self, driver: Any) -> None:
        self.driver: Any = driver

    @property
    def project(self) -> str:
        """Return the effective Google Cloud project ID."""
        val = getattr(self.driver, "project_id", None) or resolve_project_id(self.driver)
        return str(val or "")

    @property
    def dataset_id(self) -> str | None:
        """Return the default BigQuery dataset ID."""
        val = getattr(self.driver, "dataset_id", None) or resolve_dataset_id(self.driver)
        return str(val) if val is not None else None

    @property
    def location(self) -> str:
        """Return the configured BigQuery processing location."""
        cfg = getattr(self.driver, "config", None)
        return str(getattr(cfg, "location", "US") or "US")

    def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_config: Any = None,
    ) -> BigQueryConnectionQueryJob:
        """Execute ``sql`` forwarding ``job_config.query_parameters`` and job options to the native driver."""
        effective_params, job_kwargs = extract_bigquery_job_config(parameters, job_config)
        rows = (
            self.driver.select(sql, effective_params, **job_kwargs)
            if job_kwargs
            else self.driver.select(sql, effective_params)
        )
        metadata = getattr(self.driver, "last_query_metadata", {}) or {}
        return BigQueryConnectionQueryJob(list(rows), metadata)

    def cursor(self, **kwargs: Any) -> Any:
        """Delegate PEP 249 cursor creation to the underlying driver."""
        return self.driver.cursor(**kwargs)

    def _after_query(self, metadata: Mapping[str, Any]) -> None:
        """Delegate post-query metadata recording to the underlying driver."""
        after_query = getattr(self.driver, "_after_query", None)
        if callable(after_query):
            after_query(metadata)

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_config: Any = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement forwarding optional ``job_config`` options."""
        effective_params, job_kwargs = extract_bigquery_job_config(parameters, job_config)
        merged_kwargs = {**job_kwargs, **kwargs}
        if merged_kwargs:
            return int(self.driver.execute(sql, effective_params, **merged_kwargs))
        return int(self.driver.execute(sql, effective_params))

    def execute_with_description(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Delegate row and column description execution to the underlying driver."""
        return cast(
            tuple[list[tuple[Any, ...]], tuple[str, ...]],
            self.driver.execute_with_description(sql, parameters, **kwargs),
        )

    def select(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Delegate typed/dict row selection to the underlying driver."""
        return list(self.driver.select(sql, parameters, **kwargs))

    def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Delegate compound procedural script execution to the underlying driver."""
        return list(self.driver.execute_script(sql, parameters))

    def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> Any:
        """Estimate bytes processed via the underlying driver's ``dry_run``."""
        return self.driver.dry_run(sql, parameters)

    def select_to_arrow(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        """Delegate zero-copy Arrow reads to the underlying driver."""
        return self.driver.select_to_arrow(sql, parameters, **kwargs)

    def load_from_arrow(self, table_name: str, data: Any, **kwargs: Any) -> int:
        """Delegate Storage Write API Arrow loads to the underlying driver."""
        return int(self.driver.load_from_arrow(table_name, data, **kwargs))

    def begin(self) -> None:
        """Begin a BigQuery multi-statement session."""
        self.driver.begin()

    def commit(self) -> None:
        """Commit the active BigQuery multi-statement session."""
        self.driver.commit()

    def rollback(self) -> None:
        """Rollback the active BigQuery multi-statement session."""
        self.driver.rollback()

    def close(self) -> None:
        """Close the underlying native BigQuery driver."""
        close_fn = getattr(self.driver, "close", None)
        if callable(close_fn):
            close_fn()

    def __enter__(self) -> Self:
        connect_fn = getattr(self.driver, "connect", None)
        if callable(connect_fn):
            connect_fn()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
