"""Zero-sidecar Google Cloud SQL (:3307) mTLS and IAM authentication dialer."""

from __future__ import annotations

import asyncio
import socket
import ssl
import time
from collections.abc import Callable
from typing import Any, ClassVar, Self
from urllib.parse import parse_qs, urlparse

import anyio

from vertebra.db.dialer._base import (
    DEFAULT_CONNECTOR_RETRY_COUNT,
    DEFAULT_CONNECTOR_RETRY_DELAY,
    DEFAULT_CONNECTOR_RETRY_MAX_DELAY,
    VALID_GCP_IP_TYPES,
    EphemeralCertBundle,
    _BaseGCPDialer,
    generate_ephemeral_keypair,
    parse_connector_retry_count,
    parse_connector_retry_delay,
    validate_connector_retry_config,
)
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "AsyncCloudSQLDialer",
    "CloudSQLConfig",
    "CloudSQLInstanceURI",
    "EphemeralCertBundle",
    "SyncCloudSQLDialer",
    "generate_ephemeral_keypair",
)


class CloudSQLInstanceURI(FrozenBaseStruct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Parsed Google Cloud SQL instance connection name (`project:region:instance`)."""

    project: str
    """Google Cloud project ID (including optional domain prefix)."""
    region: str
    """Google Cloud region hosting the Cloud SQL instance."""
    instance: str
    """Cloud SQL instance resource ID."""

    @property
    def expected_server_cn(self) -> str:
        """Expected server certificate Subject CN (`project:instance`) on port 3307."""
        return f"{self.project}:{self.instance}"

    @classmethod
    def parse(cls, raw: str) -> CloudSQLInstanceURI:
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


class CloudSQLConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Configuration for zero-sidecar Cloud SQL connections on port 3307."""

    url_schemes: ClassVar[tuple[str, ...]] = ("cloudsql",)

    instance_connection_name: str
    """Full Cloud SQL instance connection name (`project:region:instance`)."""
    enable_iam_auth: bool = True
    """When True, authenticate using IAM database authentication tokens."""
    ip_type: str = "PUBLIC"
    """Preferred Cloud SQL IP address type (`"PUBLIC"`, `"PRIVATE"`, or `"PSC"`)."""
    admin_api_endpoint: str = "https://sqladmin.googleapis.com"
    """Base URL for the Cloud SQL Admin v1beta4 REST API."""
    refresh_lead_seconds: int = 240
    """Lead time in seconds before certificate expiry at which background renewal triggers."""
    port: int = 3307
    """Cloud SQL Auth Proxy secure mTLS server port (default `3307`)."""
    connector_retry_count: int = DEFAULT_CONNECTOR_RETRY_COUNT
    """Maximum retry attempts on transient Cloud SQL dialer errors (defaults to `3`; `0` disables retries)."""
    connector_retry_delay: float = DEFAULT_CONNECTOR_RETRY_DELAY
    """Initial backoff delay in seconds before jittered exponential dialer retries (defaults to `0.5`)."""
    connector_retry_max_delay: float = DEFAULT_CONNECTOR_RETRY_MAX_DELAY
    """Upper bound in seconds on exponential backoff between dialer retry attempts (defaults to `5.0`)."""

    def __post_init__(self) -> None:
        """Validate Cloud SQL IP connectivity type, certificate refresh lead, and retry bounds."""
        if self.ip_type not in VALID_GCP_IP_TYPES:
            msg = f"Invalid Cloud SQL ip_type {self.ip_type!r}; expected one of 'PUBLIC', 'PRIVATE', 'PSC'"
            raise ValueError(msg)
        if self.refresh_lead_seconds < 0:
            msg = f"refresh_lead_seconds must be >= 0, got {self.refresh_lead_seconds}"
            raise ValueError(msg)
        validate_connector_retry_config(
            self.connector_retry_count,
            self.connector_retry_delay,
            self.connector_retry_max_delay,
        )

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `CloudSQLConfig` from a `cloudsql://project:region:instance` URI."""
        parsed = urlparse(url)
        instance_uri = f"{parsed.netloc}{parsed.path}"
        query = parse_qs(parsed.query)
        kwargs: dict[str, Any] = dict(overrides)
        if "connector_retry_count" in query and "connector_retry_count" not in kwargs:
            kwargs["connector_retry_count"] = parse_connector_retry_count(query["connector_retry_count"][0])
        if "connector_retry_delay" in query and "connector_retry_delay" not in kwargs:
            kwargs["connector_retry_delay"] = parse_connector_retry_delay(
                query["connector_retry_delay"][0], field_name="connector_retry_delay"
            )
        if "connector_retry_max_delay" in query and "connector_retry_max_delay" not in kwargs:
            kwargs["connector_retry_max_delay"] = parse_connector_retry_delay(
                query["connector_retry_max_delay"][0], field_name="connector_retry_max_delay"
            )
        return cls(instance_connection_name=instance_uri, **kwargs)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-dialer fingerprint tuple."""
        return ("cloudsql", self.instance_connection_name)


class _BaseCloudSQLDialer(_BaseGCPDialer):
    """Base zero-sidecar Cloud SQL TLS 1.3 dialer foundation."""

    _connector_name: ClassVar[str] = "cloud_sql"
    _tls_temp_prefix: ClassVar[str] = "vtb-csql-tls-"

    __slots__ = ()

    def __init__(
        self,
        config: CloudSQLConfig,
        *,
        token_cache: Any = None,
        default_cache_cls_name: str | None = None,
    ) -> None:
        super().__init__(
            config,
            CloudSQLInstanceURI.parse(config.instance_connection_name),
            token_cache=token_cache,
            default_cache_cls_name=default_cache_cls_name,
        )

    def _build_ephemeral_cert_request(
        self,
        public_key_pem: str,
        access_token: str,
    ) -> tuple[str, bytes, dict[str, str]]:
        url = self._uri.generate_ephemeral_cert_url(self._config.admin_api_endpoint)
        payload = encode_json_bytes({"public_key": public_key_pem, "access_token": access_token})
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        return url, payload, headers

    @staticmethod
    def _parse_ephemeral_cert_response(resp_bytes: bytes, private_key_pem: str) -> EphemeralCertBundle:
        data = decode_json(resp_bytes)
        ephemeral_cert = ""
        ca_cert = ""
        server_ip = "127.0.0.1"
        if isinstance(data, dict):
            raw_ephemeral = data.get("ephemeralCert")
            ephemeral_cert = (
                raw_ephemeral.get("cert", "")
                if isinstance(raw_ephemeral, dict)
                else (raw_ephemeral if isinstance(raw_ephemeral, str) else "")
            )
            raw_ca = data.get("serverCaCert")
            ca_cert = (
                raw_ca.get("cert", "") if isinstance(raw_ca, dict) else (raw_ca if isinstance(raw_ca, str) else "")
            )
            raw_ip = data.get("ipAddress")
            if isinstance(raw_ip, str):
                server_ip = raw_ip
        return EphemeralCertBundle(
            ca_cert_pem=ca_cert,
            client_cert_pem=ephemeral_cert,
            private_key_pem=private_key_pem,
            server_ip=server_ip,
            expiration_ts=time.time() + 3600,
        )


class SyncCloudSQLDialer(_BaseCloudSQLDialer):
    """Synchronous zero-sidecar Cloud SQL TLS 1.3 dialer with proactive IAM token and ephemeral cert caching."""

    __slots__ = ()

    def __init__(self, config: CloudSQLConfig, *, token_cache: Any = None) -> None:
        super().__init__(config, token_cache=token_cache, default_cache_cls_name="SyncGcpTokenCache")

    def refresh_certificate(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        """Exchange an ephemeral client RSA/ECDSA public key for a signed Cloud SQL client certificate."""
        return self._refresh_bundle_sync(
            self._build_ephemeral_cert_request,
            self._parse_ephemeral_cert_response,
            public_key_pem=public_key_pem,
            private_key_pem=private_key_pem,
            access_token=access_token,
            http_transport=http_transport,
        )

    def connect(
        self,
        host: str | None = None,
        *,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Establish a synchronous TLS 1.3 mTLS connection to Cloud SQL with transient backoff retries."""
        target_host = host or "127.0.0.1"
        ctx = self._resolve_ssl_context(ssl_context)

        def _dial() -> ssl.SSLSocket:
            raw_sock = socket.create_connection((target_host, self.port), timeout=timeout)
            try:
                return ctx.wrap_socket(raw_sock, server_hostname=self._uri.expected_server_cn)
            except BaseException:
                raw_sock.close()
                raise

        return self._run_with_retry_sync("connect", _dial)

    def close(self) -> None:
        """Clear cached tokens and ephemeral certificates."""
        self._clear_cached_credentials()

    def __call__(
        self,
        host: str | None = None,
        *,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Callable dialer entrypoint returning a synchronous TLS 1.3 mTLS socket."""
        return self.connect(host, ssl_context=ssl_context, timeout=timeout)


class AsyncCloudSQLDialer(_BaseCloudSQLDialer):
    """Asynchronous zero-sidecar Cloud SQL TLS 1.3 dialer with proactive IAM token and ephemeral cert caching."""

    __slots__ = ("_refresh_lock",)

    def __init__(self, config: CloudSQLConfig, *, token_cache: Any = None) -> None:
        super().__init__(config, token_cache=token_cache, default_cache_cls_name="AsyncGcpTokenCache")
        self._refresh_lock = anyio.Lock()

    async def refresh_certificate(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], Any] | None = None,
    ) -> EphemeralCertBundle:
        """Asynchronously exchange an ephemeral client public key for a signed Cloud SQL client certificate."""
        async with self._refresh_lock:
            return await self._refresh_bundle_async(
                self._build_ephemeral_cert_request,
                self._parse_ephemeral_cert_response,
                public_key_pem=public_key_pem,
                private_key_pem=private_key_pem,
                access_token=access_token,
                http_transport=http_transport,
            )

    async def connect(
        self,
        host: str | None = None,
        *,
        ssl_context: ssl.SSLContext | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish an asynchronous TLS 1.3 mTLS connection to Cloud SQL with transient backoff retries."""
        target_host = host or "127.0.0.1"
        ctx = self._resolve_ssl_context(ssl_context)

        async def _dial() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            return await asyncio.open_connection(
                target_host,
                self.port,
                ssl=ctx,
                server_hostname=self._uri.expected_server_cn,
            )

        return await self._run_with_retry_async("connect", _dial)

    async def close(self) -> None:
        """Clear cached tokens and ephemeral certificates."""
        self._clear_cached_credentials()
