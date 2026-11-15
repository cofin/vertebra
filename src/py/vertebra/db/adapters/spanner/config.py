"""Configuration structs for the Google Cloud Spanner driver."""

from __future__ import annotations

import os
from typing import Any, ClassVar, Final, Literal, Self
from urllib.parse import parse_qs, urlparse

from vertebra.db.driver._common import (
    SPANGRES_DIALECT_TAG,
    SPANNER_GOOGLESQL_DIALECT_TAG,
    BaseDatabaseConfig,
)
from vertebra.typing import (
    ImproperConfigurationError,
)
from vertebra.utils.serializers import encode_json

__all__ = (
    "DEFAULT_SPANNER_ENDPOINT",
    "SPANGRES_DIALECT_TAG",
    "SPANNER_GOOGLESQL_DIALECT_TAG",
    "SpannerConfig",
)

DEFAULT_SPANNER_ENDPOINT: Final[str] = "https://spanner.googleapis.com"
_ACCEPTED_SPANNER_GOOGLESQL_SCHEMES: Final[frozenset[str]] = frozenset(
    {
        "spanner",
        "spanner+googlesql",
        "spanner+vtb",
        "spanner+vertebra",
        "spanner+vtb_async",
        "spanner+vertebra_async",
    }
)
_ACCEPTED_SPANGRES_SCHEMES: Final[frozenset[str]] = frozenset(
    {
        "spangres",
        "spanner+spangres",
        "spangres+vtb",
        "spangres+vertebra",
        "spangres+vtb_async",
        "spangres+vertebra_async",
    }
)
_ACCEPTED_SPANNER_SCHEMES: Final[frozenset[str]] = _ACCEPTED_SPANNER_GOOGLESQL_SCHEMES | _ACCEPTED_SPANGRES_SCHEMES


class SpannerConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for `SyncSpannerDriver` and `AsyncSpannerDriver`."""

    dialect_tag: ClassVar[int] = SPANNER_GOOGLESQL_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("spanner", "spangres", "spanner+googlesql", "spanner+spangres")
    driver_module: ClassVar[str] = "vertebra.db.adapters.spanner.driver"
    sync_driver_name: ClassVar[str] = "SyncSpannerDriver"
    async_driver_name: ClassVar[str] = "AsyncSpannerDriver"

    project_id: str = ""
    """Google Cloud project identifier owning the Spanner instance."""

    instance_id: str = ""
    """Cloud Spanner instance identifier."""

    database_id: str = ""
    """Cloud Spanner database identifier."""

    dsn: str | None = None
    """Optional `spanner://` or `spangres://` connection URI."""

    database_role: str | None = None
    """Optional fine-grained access control (FGAC) `creator_role` attached to the multiplexed session."""

    dialect: Literal["googlesql", "spangres"] = "googlesql"
    """SQL dialect configured on the Spanner database (`googlesql` or `spangres`)."""

    endpoint: str = DEFAULT_SPANNER_ENDPOINT
    """Cloud Spanner v1 gRPC endpoint URI."""

    admin_endpoint: str | None = None
    """REST endpoint for DDL (`UpdateDatabaseDdl`). Defaults to `https://spanner.googleapis.com` for Google endpoints and to port 9020 of an `http://…:9010` emulator endpoint; required for any other endpoint."""

    pool_size: int = 4
    """Number of pooled HTTP/2 `h2` channels maintained by `GrpcChannelPool`."""

    route_to_leader_enabled: bool = True
    """Whether `x-goog-spanner-route-to-leader: true` is sent on `ReadWrite` and `PartitionedDml` RPCs."""

    max_commit_delay_ms: int | None = None
    """Optional `CommitRequest.max_commit_delay` batching window in milliseconds."""

    data_boost_enabled: bool = True
    """Whether `ExecuteSqlRequest.data_boost_enabled` is set on partitioned Arrow reads."""

    max_partitions: int = 4
    """Maximum parallel `PartitionQuery` partitions requested for `fetch_arrow`."""

    credentials_path: str | None = None
    """Optional filesystem path to a Google Cloud service-account or ADC JSON file."""

    service_account_key: str | dict[str, Any] | None = None
    """Optional inline service-account JSON string or mapping."""

    quota_project_id: str | None = None
    """Optional `x-goog-user-project` quota project header override."""

    impersonate_service_account: str | None = None
    """Optional target service-account email for keyless IAM Credentials `generateAccessToken` impersonation."""

    use_gcloud_cli: bool = False
    """Whether to force token minting via the local `gcloud` CLI subprocess."""

    scopes: tuple[str, ...] = (
        "https://www.googleapis.com/auth/spanner.data",
        "https://www.googleapis.com/auth/spanner.admin",
        "https://www.googleapis.com/auth/cloud-platform",
    )
    """OAuth2 scopes requested when minting bearer tokens."""

    access_token: str | None = None
    """Optional static bearer token override for testing or workload federation."""

    use_shm_cache: bool = True
    """Whether to enable the cross-worker POSIX shared-memory statement cache."""

    statement_cache_capacity: int = 512
    """Maximum entries retained in the shared-memory SQL statement plan cache."""

    ca_pem: bytes | None = None
    """PEM root certificates for a non-googleapis `https://` endpoint (Spanner Omni or a private test server). Rejected for `*.googleapis.com` endpoints, which use the system trust store, and for `http://` endpoints."""

    broker: Any = None
    """Optional shared-memory `LeaseBroker` instance."""

    @property
    def database_uri(self) -> str:
        """Return the canonical `projects/{p}/instances/{i}/databases/{d}` resource path."""
        p = self.effective_project_id()
        i = self.effective_instance_id()
        d = self.effective_database_id()
        return f"projects/{p}/instances/{i}/databases/{d}"

    @property
    def database_path(self) -> str:
        """Return the canonical `projects/{p}/instances/{i}/databases/{d}` resource path."""
        return self.database_uri

    @classmethod
    def from_dsn(cls, dsn: str, **overrides: Any) -> Self:
        """Construct a `SpannerConfig` from a `spanner://` or `spangres://` URI."""
        raw_scheme, sep, rest = dsn.partition("://")
        parsed = urlparse(f"{raw_scheme.replace('_', '-')}://{rest}" if sep and "_" in raw_scheme else dsn)
        scheme = (raw_scheme if sep else parsed.scheme).lower()
        if scheme not in _ACCEPTED_SPANNER_SCHEMES:
            msg = f"Unsupported Spanner DSN scheme {raw_scheme or parsed.scheme!r}; expected 'spanner://' or 'spangres://'"
            raise ImproperConfigurationError(msg)

        dialect: Literal["googlesql", "spangres"] = "spangres" if scheme in _ACCEPTED_SPANGRES_SCHEMES else "googlesql"

        raw_segments = [seg for seg in [parsed.netloc.strip(), *parsed.path.split("/")] if seg]
        project_id = ""
        instance_id = ""
        database_id = ""

        if (
            len(raw_segments) >= 6
            and raw_segments[0] == "projects"
            and raw_segments[2] == "instances"
            and raw_segments[4] == "databases"
        ):
            project_id = raw_segments[1]
            instance_id = raw_segments[3]
            database_id = raw_segments[5]
        elif len(raw_segments) >= 3:
            project_id = raw_segments[0]
            instance_id = raw_segments[1]
            database_id = raw_segments[2]

        query_params = parse_qs(parsed.query, keep_blank_values=False)
        kwargs: dict[str, Any] = {
            "dsn": dsn,
            "dialect": dialect,
        }
        if project_id:
            kwargs["project_id"] = project_id
        if instance_id:
            kwargs["instance_id"] = instance_id
        if database_id:
            kwargs["database_id"] = database_id

        if "project_id" in query_params:
            kwargs["project_id"] = query_params["project_id"][-1]
        if "instance_id" in query_params:
            kwargs["instance_id"] = query_params["instance_id"][-1]
        if "database_id" in query_params:
            kwargs["database_id"] = query_params["database_id"][-1]
        if "database_role" in query_params:
            kwargs["database_role"] = query_params["database_role"][-1]
        if "role" in query_params:
            kwargs["database_role"] = query_params["role"][-1]
        if "dialect" in query_params:
            dl = query_params["dialect"][-1].strip().lower()
            kwargs["dialect"] = "spangres" if dl in {"spangres", "postgresql", "pg"} else "googlesql"
        if "endpoint" in query_params:
            kwargs["endpoint"] = query_params["endpoint"][-1]
        if "admin_endpoint" in query_params:
            kwargs["admin_endpoint"] = query_params["admin_endpoint"][-1]
        if "pool_size" in query_params:
            kwargs["pool_size"] = int(query_params["pool_size"][-1])
        if "route_to_leader" in query_params:
            kwargs["route_to_leader_enabled"] = query_params["route_to_leader"][-1].lower() in {
                "1",
                "true",
                "yes",
            }
        if "route_to_leader_enabled" in query_params:
            kwargs["route_to_leader_enabled"] = query_params["route_to_leader_enabled"][-1].lower() in {
                "1",
                "true",
                "yes",
            }
        if "max_commit_delay_ms" in query_params:
            kwargs["max_commit_delay_ms"] = int(query_params["max_commit_delay_ms"][-1])
        if "data_boost" in query_params:
            kwargs["data_boost_enabled"] = query_params["data_boost"][-1].lower() in {
                "1",
                "true",
                "yes",
            }
        if "data_boost_enabled" in query_params:
            kwargs["data_boost_enabled"] = query_params["data_boost_enabled"][-1].lower() in {
                "1",
                "true",
                "yes",
            }
        if "max_partitions" in query_params:
            kwargs["max_partitions"] = int(query_params["max_partitions"][-1])
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

        kwargs.update(overrides)
        return cls(**kwargs)

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `SpannerConfig` from a `spanner://` or `spangres://` URI."""
        return cls.from_dsn(url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        return (
            "spanner",
            self.database_uri,
            self.database_role or "",
            self.dialect,
            self.endpoint,
        )

    def effective_project_id(self) -> str:
        """Resolve the Google Cloud project ID from config or environment."""
        if self.project_id.strip():
            return self.project_id.strip()
        for env_key in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT", "CLOUDSDK_CORE_PROJECT"):
            val = os.environ.get(env_key, "").strip()
            if val:
                return val
        if self.endpoint.startswith("http://"):
            return "test-project"
        raise ImproperConfigurationError(
            "SpannerConfig requires a non-empty project_id or GOOGLE_CLOUD_PROJECT environment variable"
        )

    def effective_instance_id(self) -> str:
        """Resolve the Cloud Spanner instance ID from config or environment."""
        if self.instance_id.strip():
            return self.instance_id.strip()
        env_val = os.environ.get("SPANNER_INSTANCE_ID", "").strip()
        if env_val:
            return env_val
        if self.endpoint.startswith("http://"):
            return "test-instance"
        raise ImproperConfigurationError("SpannerConfig requires a non-empty instance_id")

    def effective_database_id(self) -> str:
        """Resolve the Cloud Spanner database ID from config or environment."""
        if self.database_id.strip():
            return self.database_id.strip()
        env_val = os.environ.get("SPANNER_DATABASE_ID", "").strip()
        if env_val:
            return env_val
        if self.endpoint.startswith("http://"):
            return "test-database"
        raise ImproperConfigurationError("SpannerConfig requires a non-empty database_id")

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
            self.endpoint.startswith("http://")
            and not self.credentials_path
            and self.service_account_key is None
            and not self.access_token
            and not self.impersonate_service_account
            and not self.use_gcloud_cli
        ):
            opts["allow_ambient_adc"] = "false"
        return opts
