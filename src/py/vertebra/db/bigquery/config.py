"""Configuration and value structs for the Google Cloud BigQuery driver."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Final, Literal, Self
from urllib.parse import parse_qs, urlparse

import msgspec

from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.typing import (
    FrozenBaseStruct,
    ImproperConfigurationError,
)
from vertebra.utils.serializers import encode_json

__all__ = (
    "BIGQUERY_DIALECT_TAG",
    "DEFAULT_BIGQUERY_REST_ENDPOINT",
    "DEFAULT_BIGQUERY_STORAGE_ENDPOINT",
    "BigQueryConfig",
    "BigQueryDryRunResult",
    "ObjectRef",
    "ObjectTableConfig",
)

BIGQUERY_DIALECT_TAG: Final[int] = 7
DEFAULT_BIGQUERY_REST_ENDPOINT: Final[str] = "https://bigquery.googleapis.com"
DEFAULT_BIGQUERY_STORAGE_ENDPOINT: Final[str] = "https://bigquerystorage.googleapis.com:443"


class BigQueryConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for `SyncBigQueryDriver` and `AsyncBigQueryDriver`."""

    dialect: ClassVar[str] = "bigquery"
    dialect_tag: ClassVar[int] = BIGQUERY_DIALECT_TAG

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
    """Optional PEM-encoded root certificate bundle for custom TLS verification."""

    broker: Any = None
    """Optional shared-memory `LeaseBroker` instance."""

    @classmethod
    def from_dsn(cls, dsn: str, **overrides: Any) -> Self:
        """Construct a `BigQueryConfig` from a `bigquery://` DSN URI and keyword overrides."""
        parsed = urlparse(dsn)
        scheme = parsed.scheme.lower()
        if scheme not in {"bigquery", "bq"}:
            msg = f"Unsupported BigQuery DSN scheme {parsed.scheme!r}; expected 'bigquery://'"
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
    """Zero-RTT presigned `GET` URL generated via `vertebra._storage.ObjectStore`."""


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
            f"CREATE EXTERNAL TABLE {exists_clause}{self.table_name}\n"
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
                presign_async = getattr(store, "presign_get_async", None)
                presign_sync = getattr(store, "presign_get", None)
                sign_async_fn = getattr(store, "sign_async", None)
                sign_sync_fn = getattr(store, "sign_sync", None)
                try:
                    if callable(presign_async):
                        signed_url = str(await presign_async(object_key, expires_in_secs=expires_in_secs))
                    elif callable(sign_async_fn):
                        signed_url = str(await sign_async_fn(object_key, expires_in=expires_in_secs))
                    elif callable(presign_sync):
                        signed_url = str(presign_sync(object_key, expires_in_secs=expires_in_secs))
                    elif callable(sign_sync_fn):
                        signed_url = str(sign_sync_fn(object_key, expires_in=expires_in_secs))
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
