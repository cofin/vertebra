"""Configuration and value structs for the Google Cloud BigQuery driver."""

from __future__ import annotations

import os
from typing import Any, ClassVar, Final, Literal, Self
from urllib.parse import parse_qs, urlparse

import msgspec

from vertebra.db.driver._common import BIGQUERY_DIALECT_TAG, BaseDatabaseConfig
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
