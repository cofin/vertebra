"""Zero-sidecar Google Cloud SQL (:3307) mTLS and IAM authentication dialer."""

from __future__ import annotations

import ssl
import time

import msgspec

__all__ = (
    "CloudSqlConfig",
    "CloudSqlDialer",
    "CloudSqlInstanceUri",
)


class CloudSqlInstanceUri(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Parsed Google Cloud SQL instance connection name (`project:region:instance`)."""

    project: str
    region: str
    instance: str

    @property
    def expected_server_cn(self) -> str:
        """Expected server certificate Subject CN (`project:instance`) on port 3307."""
        return f"{self.project}:{self.instance}"

    @classmethod
    def parse(cls, raw: str) -> CloudSqlInstanceUri:
        """Parse `project:region:instance` (supporting domain-scoped projects)."""
        trimmed = raw.strip()
        parts = trimmed.split(":")
        if len(parts) == 3 and all(parts):
            return cls(project=parts[0], region=parts[1], instance=parts[2])
        if len(parts) == 4 and all(parts):
            return cls(project=f"{parts[0]}:{parts[1]}", region=parts[2], instance=parts[3])
        msg = f"Invalid Cloud SQL instance connection name {raw!r}; expected 'project:region:instance'"
        raise ValueError(msg)

    def generate_ephemeral_cert_url(self, base_endpoint: str = "https://sqladmin.googleapis.com") -> str:
        """Return the Cloud SQL Admin API v1beta4 generateEphemeralCert endpoint URL."""
        base = base_endpoint.rstrip("/")
        return f"{base}/sql/v1beta4/projects/{self.project}/instances/{self.instance}:generateEphemeralCert"


class CloudSqlConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for zero-sidecar Cloud SQL connections on port 3307."""

    instance_connection_name: str
    enable_iam_auth: bool = True
    ip_type: str = "PUBLIC"
    admin_api_endpoint: str = "https://sqladmin.googleapis.com"
    refresh_Lead_seconds: int = 240
    port: int = 3307


class CloudSqlDialer:
    """Zero-sidecar Cloud SQL TLS 1.3 dialer with proactive IAM token and ephemeral cert caching."""

    __slots__ = ("_cached_expiry", "_cached_token", "_config", "_uri")

    def __init__(self, config: CloudSqlConfig) -> None:
        self._config = config
        self._uri = CloudSqlInstanceUri.parse(config.instance_connection_name)
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0

    @property
    def uri(self) -> CloudSqlInstanceUri:
        """Return the parsed CloudSqlInstanceUri."""
        return self._uri

    @property
    def port(self) -> int:
        """Return the Cloud SQL Auth Proxy TLS port (default 3307)."""
        return self._config.port

    def needs_refresh(self, now_ts: float | None = None) -> bool:
        """Return True if the cached IAM token or ephemeral cert is within the refresh lead window."""
        current = time.time() if now_ts is None else now_ts
        return self._cached_token is None or (self._cached_expiry - current) <= self._config.refresh_Lead_seconds

    def cache_iam_token(self, token: str, expires_in_seconds: int = 3600, *, now_ts: float | None = None) -> None:
        """Store a refreshed OAuth2 IAM database login token."""
        current = time.time() if now_ts is None else now_ts
        self._cached_token = token
        self._cached_expiry = current + expires_in_seconds

    def build_tls_context(
        self,
        *,
        ca_pem: str | None = None,
        client_cert_pem: str | None = None,
        client_key_pem: str | None = None,
    ) -> ssl.SSLContext:
        """Build a TLS 1.3 SSLContext for Cloud SQL port 3307.

        Cloud SQL instance certificates use `project:instance` in the Subject CN rather
        than a DNS hostname, so `check_hostname` is disabled while certificate chain
        verification remains enforced when `ca_pem` is provided.
        """
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.check_hostname = False
        if ca_pem:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.load_verify_locations(cadata=ca_pem)
        else:
            ctx.verify_mode = ssl.CERT_NONE
        if client_cert_pem and client_key_pem:
            raise NotImplementedError("Use load_cert_chain with a temporary memfd/file when client PEMs are provided")
        return ctx
