"""Configuration and value structs for the Google Cloud BigQuery driver."""

from __future__ import annotations

import inspect
import os
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Final, Literal, Self
from urllib.parse import parse_qs, urlparse

import msgspec

from vertebra.db.driver._common import BIGQUERY_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import (
    FrozenBaseStruct,
    ImproperConfigurationError,
)
from vertebra.utils.serializers import encode_json
from vertebra.utils.text import split_qualified_identifier, validate_identifier

__all__ = (
    "BIGQUERY_DIALECT_TAG",
    "DEFAULT_BIGQUERY_REST_ENDPOINT",
    "DEFAULT_BIGQUERY_STORAGE_ENDPOINT",
    "BigQueryConfig",
    "BigQueryDryRunResult",
    "BigQueryTableIdentifier",
    "ObjectRef",
    "ObjectTableConfig",
)

DEFAULT_BIGQUERY_REST_ENDPOINT: Final[str] = "https://bigquery.googleapis.com"
DEFAULT_BIGQUERY_STORAGE_ENDPOINT: Final[str] = "https://bigquerystorage.googleapis.com:443"
_ACCEPTED_BIGQUERY_SCHEMES: Final[frozenset[str]] = frozenset(
    {
        "bigquery",
        "bq",
        "bigquery+vtb",
        "bigquery+vertebra",
        "bigquery+vtb_async",
        "bigquery+vertebra_async",
        "bq+vtb",
        "bq+vertebra",
        "bq+vtb_async",
        "bq+vertebra_async",
    }
)


class BigQueryConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for `SyncBigQueryDriver` and `AsyncBigQueryDriver`."""

    dialect: ClassVar[str] = "bigquery"
    dialect_tag: ClassVar[int] = BIGQUERY_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("bigquery", "bq")
    driver_module: ClassVar[str] = "vertebra.db.adapters.bigquery.driver"
    sync_driver_name: ClassVar[str] = "SyncBigQueryDriver"
    async_driver_name: ClassVar[str] = "AsyncBigQueryDriver"

    project_id: str = ""
    """Google Cloud project identifier owning BigQuery jobs and default datasets."""

    dataset_id: str | None = None
    """Default BigQuery dataset identifier for unqualified table references."""

    location: str = "US"
    """BigQuery dataset and job processing location (such as `US` or `EU`)."""

    dsn: str | None = None
    """Optional `bigquery://project_id[/dataset_id]?location=...` connection URI."""

    rest_endpoint: str = DEFAULT_BIGQUERY_REST_ENDPOINT
    """Base URL for the BigQuery REST v2 API."""

    storage_endpoint: str = DEFAULT_BIGQUERY_STORAGE_ENDPOINT
    """Endpoint URL for the BigQuery Storage Read/Write v1 gRPC API."""

    maximum_bytes_billed: int | None = None
    """Optional FinOps byte billing cap applied to executed query jobs."""

    job_timeout_ms: int | None = None
    """Optional server-side query execution timeout in milliseconds."""

    priority: Literal["INTERACTIVE", "BATCH"] = "INTERACTIVE"
    """BigQuery query scheduling priority (`INTERACTIVE` or `BATCH`)."""

    labels: dict[str, str] = msgspec.field(default_factory=dict)
    """Static key-value labels attached to BigQuery jobs after label sanitization."""

    credentials_path: str | None = None
    """Optional filesystem path to a Google Cloud service-account or ADC JSON file."""

    service_account_key: str | dict[str, Any] | None = None
    """Optional inline service-account JSON string or mapping."""

    quota_project_id: str | None = None
    """Optional `x-goog-user-project` quota project override."""

    impersonate_service_account: str | None = None
    """Optional target service-account email for keyless IAM Credentials `generateAccessToken` impersonation."""

    access_token: str | None = None
    """Optional static OAuth2 bearer access token override."""

    use_gcloud_cli: bool = False
    """Whether to force token minting via the local `gcloud` CLI subprocess."""

    scopes: tuple[str, ...] = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
    )
    """OAuth2 scopes requested when minting bearer tokens."""

    pool_size: int = 4
    """Maximum pooled HTTP/1.1 and HTTP/2 `h2` channels maintained by the Rust client."""

    statement_cache_capacity: int = 256
    """Maximum entries retained in the shared-memory SQL statement plan cache."""

    use_shm_cache: bool = True
    """Whether to enable the cross-worker POSIX shared-memory statement cache."""

    arrow_compression: Literal["LZ4_FRAME", "ZSTD", "UNSPECIFIED"] = "LZ4_FRAME"
    """Arrow IPC stream compression codec negotiated with BigQuery Storage Read API v1."""

    max_read_streams: int = 4
    """Maximum parallel `ReadRows` streams requested in `CreateReadSession`."""

    ca_pem: bytes | None = None
    """PEM root certificates added to the REST API client. The Storage Read/Write API clients use the system trust store and ignore this value."""

    broker: Any = None
    """Optional shared-memory `LeaseBroker` instance."""

    @classmethod
    def from_dsn(cls, dsn: str, **overrides: Any) -> Self:
        """Construct a `BigQueryConfig` from a `bigquery://` DSN URI and keyword overrides."""
        raw_scheme, sep, rest = dsn.partition("://")
        parsed = urlparse(f"{raw_scheme.replace('_', '-')}://{rest}" if sep and "_" in raw_scheme else dsn)
        scheme = (raw_scheme if sep else parsed.scheme).lower()
        if scheme not in _ACCEPTED_BIGQUERY_SCHEMES:
            msg = f"Unsupported BigQuery DSN scheme {raw_scheme or parsed.scheme!r}; expected 'bigquery://'"
            raise ImproperConfigurationError(msg)

        project_id = parsed.netloc.strip()
        path_parts = [seg for seg in parsed.path.split("/") if seg]
        dataset_id: str | None = path_parts[0] if path_parts else None
        if not project_id and path_parts:
            project_id = path_parts[0]
            dataset_id = path_parts[1] if len(path_parts) > 1 else None

        query_params = parse_qs(parsed.query, keep_blank_values=False)
        kwargs: dict[str, Any] = {"dsn": dsn}
        if project_id:
            kwargs["project_id"] = project_id
        if dataset_id:
            kwargs["dataset_id"] = dataset_id

        if "dataset" in query_params:
            kwargs["dataset_id"] = query_params["dataset"][-1]
        if "dataset_id" in query_params:
            kwargs["dataset_id"] = query_params["dataset_id"][-1]
        if "location" in query_params:
            kwargs["location"] = query_params["location"][-1]
        if "rest_endpoint" in query_params:
            kwargs["rest_endpoint"] = query_params["rest_endpoint"][-1]
        if "storage_endpoint" in query_params:
            kwargs["storage_endpoint"] = query_params["storage_endpoint"][-1]
        if "maximum_bytes_billed" in query_params:
            kwargs["maximum_bytes_billed"] = int(query_params["maximum_bytes_billed"][-1])
        if "job_timeout_ms" in query_params:
            kwargs["job_timeout_ms"] = int(query_params["job_timeout_ms"][-1])
        if "priority" in query_params:
            prio = query_params["priority"][-1].upper()
            if prio in {"INTERACTIVE", "BATCH"}:
                kwargs["priority"] = prio
        if "credentials_path" in query_params:
            kwargs["credentials_path"] = query_params["credentials_path"][-1]
        if "quota_project_id" in query_params:
            kwargs["quota_project_id"] = query_params["quota_project_id"][-1]
        if "impersonate_service_account" in query_params:
            kwargs["impersonate_service_account"] = query_params["impersonate_service_account"][-1]
        if "access_token" in query_params:
            kwargs["access_token"] = query_params["access_token"][-1]
        if "use_gcloud_cli" in query_params:
            kwargs["use_gcloud_cli"] = query_params["use_gcloud_cli"][-1].lower() in {"1", "true", "yes"}
        if "pool_size" in query_params:
            kwargs["pool_size"] = int(query_params["pool_size"][-1])
        if "arrow_compression" in query_params:
            comp = query_params["arrow_compression"][-1].upper()
            if comp in {"LZ4_FRAME", "ZSTD", "UNSPECIFIED"}:
                kwargs["arrow_compression"] = comp

        kwargs.update(overrides)
        return cls(**kwargs)

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `BigQueryConfig` from a `bigquery://` or `bq://` URI."""
        return cls.from_dsn(url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        return (
            "bigquery",
            self.effective_project_id(),
            self.effective_dataset_id() or "",
            self.location,
            self.rest_endpoint,
            self.storage_endpoint,
        )

    def effective_project_id(self) -> str:
        """Resolve the effective Google Cloud project ID from config, DSN, or environment."""
        if self.project_id.strip():
            return self.project_id.strip()
        if self.dsn:
            parsed = urlparse(self.dsn)
            if parsed.netloc.strip():
                return parsed.netloc.strip()
        for env_key in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT", "CLOUDSDK_CORE_PROJECT"):
            env_val = os.environ.get(env_key, "").strip()
            if env_val:
                return env_val
        if self.rest_endpoint.startswith("http://") or self.storage_endpoint.startswith("http://"):
            return "test-project"
        raise ImproperConfigurationError(
            "BigQueryConfig requires a non-empty project_id or GOOGLE_CLOUD_PROJECT environment variable"
        )

    def effective_dataset_id(self) -> str | None:
        """Resolve the default dataset ID from config or `dsn` path."""
        if self.dataset_id and self.dataset_id.strip():
            return self.dataset_id.strip()
        if self.dsn:
            parsed = urlparse(self.dsn)
            parts = [seg for seg in parsed.path.split("/") if seg]
            if parts:
                return parts[0]
        return None

    def build_auth_options(self) -> dict[str, str]:
        """Build the string dictionary passed to `GcpTokenCache::from_options` in Rust."""
        opts: dict[str, str] = {}
        if self.access_token:
            opts["access_token"] = self.access_token
        if self.credentials_path:
            opts["application_credentials_path"] = self.credentials_path
        if self.service_account_key is not None:
            if isinstance(self.service_account_key, str):
                opts["service_account_key"] = self.service_account_key
            else:
                opts["service_account_key"] = encode_json(self.service_account_key)
        if self.impersonate_service_account:
            opts["impersonate_service_account"] = self.impersonate_service_account
        if self.use_gcloud_cli:
            opts["use_gcloud_cli"] = "true"
        if self.quota_project_id:
            opts["quota_project_id"] = self.quota_project_id
        if self.scopes:
            opts["scopes"] = ",".join(self.scopes)
        if (
            (self.rest_endpoint.startswith("http://") or self.storage_endpoint.startswith("http://"))
            and not self.credentials_path
            and self.service_account_key is None
            and not self.access_token
            and not self.impersonate_service_account
            and not self.use_gcloud_cli
        ):
            opts["allow_ambient_adc"] = "false"
        return opts


class BigQueryDryRunResult(FrozenBaseStruct, frozen=True, gc=False):
    """FinOps cost estimation and output schema metadata returned by `dry_run()`."""

    total_bytes_processed: int
    """Estimated bytes scanned by the query."""

    total_bytes_billed: int
    """Estimated bytes billed by the query under on-demand pricing."""

    cache_hit: bool
    """Whether the query can be satisfied directly from the BigQuery result cache."""

    referenced_tables: tuple[str, ...] = ()
    """Fully-qualified `project.dataset.table` identifiers referenced by the query."""

    schema: tuple[dict[str, Any], ...] = ()
    """Output column schema descriptors returned by the dry-run validation pass."""

    estimated_cost_usd: float = 0.0
    """Estimated on-demand query cost in USD at `$6.25 / TiB`."""


class ObjectRef(FrozenBaseStruct, frozen=True, gc=False):
    """Metadata and optional presigned HTTP URL for an object row in a BigQuery Object Table."""

    uri: str
    """Canonical `gs://bucket/key` or multicloud object URI."""

    version: str | None = None
    """Storage object generation or version identifier."""

    content_type: str | None = None
    """MIME content type reported by the object store."""

    size: int = 0
    """Object payload size in bytes."""

    md5_hex: str | None = None
    """Hex or base64 MD5 digest reported by BigQuery Object Table metadata."""

    updated: str | None = None
    """Last-modified timestamp string or microsecond epoch representation."""

    signed_url: str | None = None
    """Zero-RTT presigned `GET` URL generated via `vertebra._storage.SyncObjectStore`."""


class ObjectTableConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Configuration for creating and querying a BigQuery External Object Table over GCS."""

    table_name: str
    """Qualified or unqualified BigQuery external table name (such as `dataset.invoices_obj`)."""

    connection_id: str
    """BigQuery Cloud Resource connection identifier (such as `us.gcs-conn`)."""

    uris: tuple[str, ...]
    """One or more `gs://bucket/prefix/*` URI patterns backing the Object Table."""

    object_metadata: Literal["SIMPLE", "DIRECTORY"] = "SIMPLE"
    """BigQuery `object_metadata` mode (`SIMPLE` or `DIRECTORY`)."""

    max_staleness: str | None = None
    """Optional metadata cache staleness interval literal (such as `INTERVAL 1 HOUR`)."""

    metadata_cache_mode: Literal["AUTOMATIC", "MANUAL"] | None = None
    """Optional BigQuery metadata cache refresh mode (`AUTOMATIC` or `MANUAL`)."""

    def create_object_table_sql(self, *, if_not_exists: bool = True) -> str:
        """Render the GoogleSQL `CREATE EXTERNAL TABLE ... WITH CONNECTION ... OPTIONS(...)` DDL."""
        if not self.uris:
            raise ImproperConfigurationError("ObjectTableConfig.uris must contain at least one URI")
        quoted_table = BigQueryTableIdentifier.validate_and_quote(self.table_name, field_name="table_name")
        validate_identifier(self.connection_id, "connection_id", allow_hyphens=True)
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        staleness_clause = f"\nWITH MAX STALENESS {self.max_staleness}" if self.max_staleness else ""
        escaped_uris = ", ".join("'" + u.replace("'", "\\'") + "'" for u in self.uris)
        options_parts = [
            f"object_metadata = '{self.object_metadata}'",
            f"uris = [{escaped_uris}]",
        ]
        if self.metadata_cache_mode is not None:
            options_parts.append(f"metadata_cache_mode = '{self.metadata_cache_mode}'")
        options_sql = ",\n  ".join(options_parts)
        return (
            f"CREATE EXTERNAL TABLE {exists_clause}{quoted_table}\n"
            f"WITH CONNECTION `{self.connection_id}`{staleness_clause}\n"
            f"OPTIONS (\n  {options_sql}\n)"
        )

    def hydrate_object_refs_sync(
        self,
        rows: Sequence[Mapping[str, Any]],
        store: Any | None = None,
        *,
        expires_in_secs: int = 3600,
        fallback_to_authenticated_url: bool = True,
    ) -> list[ObjectRef]:
        """Convert Object Table metadata rows into `ObjectRef` structs with presigned URLs."""
        refs: list[ObjectRef] = []
        for row in rows:
            uri = str(row.get("uri") or row.get("object_uri") or "")
            signed_url: str | None = None
            if store is not None and uri:
                object_key = self._extract_object_key_from_uri(uri)
                presign_fn = getattr(store, "presign_get", None)
                sign_sync_fn = getattr(store, "sign_sync", None)
                try:
                    if callable(presign_fn):
                        signed_url = str(presign_fn(object_key, expires_in_secs=expires_in_secs))
                    elif callable(sign_sync_fn):
                        signed_url = str(sign_sync_fn(object_key, expires_in=expires_in_secs))
                except Exception:
                    signed_url = None
            if signed_url is None and fallback_to_authenticated_url and uri.startswith("gs://"):
                signed_url = self._build_authenticated_gcs_browser_url(uri)
            refs.append(self._row_to_object_ref(row, signed_url=signed_url))
        return refs

    async def hydrate_object_refs_async(
        self,
        rows: Sequence[Mapping[str, Any]],
        store: Any | None = None,
        *,
        expires_in_secs: int = 3600,
        fallback_to_authenticated_url: bool = True,
    ) -> list[ObjectRef]:
        """Asynchronously convert Object Table metadata rows into `ObjectRef` structs."""
        refs: list[ObjectRef] = []
        for row in rows:
            uri = str(row.get("uri") or row.get("object_uri") or "")
            signed_url: str | None = None
            if store is not None and uri:
                object_key = self._extract_object_key_from_uri(uri)
                presign_fn = getattr(store, "presign_get", None)
                sign_fn = getattr(store, "sign_async", None) or getattr(store, "sign_sync", None)
                try:
                    if callable(presign_fn):
                        res = presign_fn(object_key, expires_in_secs=expires_in_secs)
                        if inspect.isawaitable(res):
                            res = await res
                        signed_url = str(res)
                    elif callable(sign_fn):
                        res = sign_fn(object_key, expires_in=expires_in_secs)
                        if inspect.isawaitable(res):
                            res = await res
                        signed_url = str(res)
                except Exception:
                    signed_url = None
            if signed_url is None and fallback_to_authenticated_url and uri.startswith("gs://"):
                signed_url = self._build_authenticated_gcs_browser_url(uri)
            refs.append(self._row_to_object_ref(row, signed_url=signed_url))
        return refs

    @staticmethod
    def _extract_object_key_from_uri(uri: str) -> str:
        if "://" not in uri:
            return uri.lstrip("/")
        parsed = urlparse(uri)
        return parsed.path.lstrip("/")

    @staticmethod
    def _build_authenticated_gcs_browser_url(uri: str) -> str | None:
        parsed = urlparse(uri)
        bucket = parsed.netloc.strip("/")
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            return None
        return f"https://storage.cloud.google.com/{bucket}/{key}"

    @staticmethod
    def _row_to_object_ref(
        row: Mapping[str, Any],
        *,
        signed_url: str | None = None,
    ) -> ObjectRef:
        uri = str(row.get("uri") or row.get("object_uri") or "")
        version_raw = row.get("generation") or row.get("version")
        version = str(version_raw) if version_raw is not None else None
        content_type_raw = row.get("content_type")
        content_type = str(content_type_raw) if content_type_raw is not None else None
        size_raw = row.get("size", 0)
        size = int(size_raw) if size_raw is not None else 0
        md5_raw = row.get("md5_hash") or row.get("md5_hex") or row.get("e_tag")
        md5_hex = str(md5_raw) if md5_raw is not None else None
        updated_raw = row.get("updated") or row.get("last_modified")
        updated = str(updated_raw) if updated_raw is not None else None
        return ObjectRef(
            uri=uri,
            version=version,
            content_type=content_type,
            size=size,
            md5_hex=md5_hex,
            updated=updated,
            signed_url=signed_url,
        )


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
