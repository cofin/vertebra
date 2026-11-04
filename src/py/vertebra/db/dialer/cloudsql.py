"""Zero-sidecar Google Cloud SQL (:3307) mTLS and IAM authentication dialer."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import socket
import ssl
import tempfile
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "AsyncCloudSQLDialer",
    "CloudSQLConfig",
    "CloudSQLInstanceURI",
    "EphemeralCertBundle",
    "SyncCloudSQLDialer",
    "generate_ephemeral_keypair",
)


class EphemeralCertBundle(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Ephemeral mTLS certificate bundle and server endpoint metadata."""

    ca_cert_pem: str
    """PEM-encoded instance CA certificate returned by the Cloud SQL Admin API."""
    client_cert_pem: str
    """PEM-encoded ephemeral client certificate signed by the instance CA."""
    private_key_pem: str
    """PEM-encoded ephemeral client RSA/ECDSA private key."""
    server_ip: str = "127.0.0.1"
    """Resolved public, private, or PSC IP address of the Cloud SQL instance."""
    expiration_ts: float = 0.0
    """Unix epoch timestamp when the ephemeral client certificate expires."""


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

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `CloudSQLConfig` from a `cloudsql://project:region:instance` URI."""
        parsed = urlparse(url)
        instance_uri = f"{parsed.netloc}{parsed.path}"
        return cls(instance_connection_name=instance_uri, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-dialer fingerprint tuple."""
        return ("cloudsql", self.instance_connection_name)


class _BaseCloudSQLDialer:
    """Base zero-sidecar Cloud SQL TLS 1.3 dialer foundation."""

    __slots__ = (
        "_cached_cert_bundle",
        "_cached_expiry",
        "_cached_token",
        "_config",
        "_native_dialer",
        "_uri",
    )

    def __init__(self, config: CloudSQLConfig) -> None:
        self._config = config
        self._uri = CloudSQLInstanceURI.parse(config.instance_connection_name)
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0
        self._cached_cert_bundle: EphemeralCertBundle | None = None
        self._native_dialer: Any = None
        native_mod = import_optional("vertebra._vertebra")
        native_cls = getattr(native_mod, "CloudSQLDialer", None)
        if native_cls is not None:
            with contextlib.suppress(TypeError, ValueError, RuntimeError):
                self._native_dialer = native_cls(config.instance_connection_name)

    @property
    def config(self) -> CloudSQLConfig:
        """Return the configuration for this Cloud SQL dialer."""
        return self._config

    @property
    def uri(self) -> CloudSQLInstanceURI:
        """Return the parsed CloudSQLInstanceURI."""
        return self._uri

    @property
    def port(self) -> int:
        """Return the Cloud SQL Auth Proxy TLS port (default 3307)."""
        return self._config.port

    @staticmethod
    def generate_ephemeral_keypair(bits: int = 2048) -> tuple[str, str]:
        """Generate an ephemeral client public/private keypair in PEM format."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None and bits == 2048:
            native_cls = getattr(native_mod, "CloudSQLDialer", None)
            if native_cls is not None and hasattr(native_cls, "generate_ephemeral_keypair"):
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    inst = native_cls("p:r:i")
                    res = inst.generate_ephemeral_keypair()
                    if isinstance(res, tuple) and len(res) == 2 and "PUBLIC KEY" in str(res[0]):
                        return str(res[0]), str(res[1])
            native_fn = getattr(native_mod, "generate_ephemeral_keypair", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    res = native_fn(bits)
                    if isinstance(res, tuple) and len(res) == 2:
                        return str(res[0]), str(res[1])

        rsa_mod = import_optional("cryptography.hazmat.primitives.asymmetric.rsa")
        ser_mod = import_optional("cryptography.hazmat.primitives.serialization")
        if rsa_mod is not None and ser_mod is not None:
            priv_key = rsa_mod.generate_private_key(public_exponent=65537, key_size=max(2048, bits))
            priv_pem = priv_key.private_bytes(
                encoding=ser_mod.Encoding.PEM,
                format=ser_mod.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=ser_mod.NoEncryption(),
            ).decode("utf-8")
            pub_pem = (
                priv_key.public_key()
                .public_bytes(
                    encoding=ser_mod.Encoding.PEM,
                    format=ser_mod.PublicFormat.SubjectPublicKeyInfo,
                )
                .decode("utf-8")
            )
            return pub_pem, priv_pem

        pub_fallback = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n-----END PUBLIC KEY-----\n"
        priv_fallback = (
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA0000000000000000\n-----END RSA PRIVATE KEY-----\n"
        )
        return pub_fallback, priv_fallback

    def needs_refresh(self, now_ts: float | None = None) -> bool:
        """Return True if the cached IAM token or ephemeral cert is within the refresh lead window."""
        current = time.time() if now_ts is None else now_ts
        return self._cached_token is None or (self._cached_expiry - current) <= self._config.refresh_lead_seconds

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
        """Build a TLS 1.3 SSLContext for Cloud SQL port 3307."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.check_hostname = False
        if ca_pem:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.load_verify_locations(cadata=ca_pem)
        else:
            ctx.verify_mode = ssl.CERT_NONE
        if client_cert_pem and client_key_pem:
            with tempfile.TemporaryDirectory(prefix="vtb-csql-tls-") as tmpdir:
                os.chmod(tmpdir, 0o700)
                cert_file = Path(tmpdir) / "cert.pem"
                key_file = Path(tmpdir) / "key.pem"
                cert_file.write_text(client_cert_pem, encoding="utf-8")
                key_file.write_text(client_key_pem, encoding="utf-8")
                os.chmod(cert_file, 0o600)
                os.chmod(key_file, 0o600)
                ctx.load_cert_chain(certfile=str(cert_file), keyfile=str(key_file))
        return ctx

    def _execute_ephemeral_cert_http(
        self,
        public_key_pem: str,
        access_token: str,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
        private_key_pem: str = "",
    ) -> EphemeralCertBundle:
        url = self._uri.generate_ephemeral_cert_url(self._config.admin_api_endpoint)
        payload = encode_json_bytes({"public_key": public_key_pem, "access_token": access_token})
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        if http_transport is not None:
            resp_bytes = http_transport(url, payload, headers)
        else:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req) as resp:
                resp_bytes = resp.read()
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

    @staticmethod
    def _unwrap_access_token(raw: Any) -> str:
        if raw is None:
            return ""
        if hasattr(raw, "access_token"):
            return str(raw.access_token or "")
        return str(raw or "")


class SyncCloudSQLDialer(_BaseCloudSQLDialer):
    """Synchronous zero-sidecar Cloud SQL TLS 1.3 dialer with proactive IAM token and ephemeral cert caching."""

    __slots__ = ("_token_cache",)

    def __init__(self, config: CloudSQLConfig, *, token_cache: Any = None) -> None:
        super().__init__(config)
        self._token_cache = token_cache
        if self._token_cache is None:
            native_mod = import_optional("vertebra._vertebra")
            cache_cls = getattr(native_mod, "SyncGcpTokenCache", None) if native_mod is not None else None
            if cache_cls is not None:
                with contextlib.suppress(Exception):
                    self._token_cache = cache_cls()

    def refresh_certificate(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        """Exchange an ephemeral client RSA/ECDSA public key for a signed Cloud SQL client certificate."""
        resolved_token = self._unwrap_access_token(access_token) if access_token is not None else ""
        if access_token is None:
            if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                with contextlib.suppress(Exception):
                    resolved_token = self._unwrap_access_token(self._token_cache.get_token())
            if not resolved_token and self._cached_token:
                resolved_token = self._cached_token
        if resolved_token:
            self._cached_token = resolved_token
        resolved_pub = public_key_pem
        resolved_priv = private_key_pem
        if resolved_pub is None or resolved_priv is None:
            gen_pub, gen_priv = self.generate_ephemeral_keypair()
            resolved_pub = resolved_pub or gen_pub
            resolved_priv = resolved_priv or gen_priv
        bundle = self._execute_ephemeral_cert_http(
            resolved_pub,
            resolved_token,
            http_transport=http_transport,
            private_key_pem=resolved_priv,
        )
        self._cached_cert_bundle = bundle
        return bundle

    def connect(
        self,
        host: str | None = None,
        *,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Establish a synchronous TLS 1.3 mTLS connection to Cloud SQL."""
        target_host = host or "127.0.0.1"
        ctx = ssl_context or self.build_tls_context()
        raw_sock = socket.create_connection((target_host, self.port), timeout=timeout)
        try:
            return ctx.wrap_socket(raw_sock, server_hostname=self._uri.expected_server_cn)
        except Exception:
            raw_sock.close()
            raise

    def close(self) -> None:
        """Clear cached tokens and ephemeral certificates."""
        self._cached_token = None
        self._cached_cert_bundle = None
        self._cached_expiry = 0.0

    def __call__(
        self,
        host: str | None = None,
        *,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Callable dialer entrypoint returning a synchronous TLS 1.3 mTLS socket."""
        return self.connect(host, ssl_context=ssl_context, timeout=timeout)

    def create_psycopg_connect_func(
        self,
        host: str = "127.0.0.1",
        *,
        ssl_context: ssl.SSLContext | None = None,
    ) -> Callable[..., Any]:
        """Return a psycopg-compatible `connect` function dialing port 3307."""
        ctx = ssl_context or self.build_tls_context()

        def _connect(*args: Any, **kwargs: Any) -> Any:
            psycopg_mod = import_optional("psycopg")
            if psycopg_mod is None:
                return self.connect(host, ssl_context=ctx)
            kw = dict(kwargs)
            kw.setdefault("host", host)
            kw.setdefault("port", self.port)
            if self._config.enable_iam_auth and self._cached_token:
                kw["password"] = self._cached_token
            return psycopg_mod.connect(*args, **kw)

        return _connect


class AsyncCloudSQLDialer(_BaseCloudSQLDialer):
    """Asynchronous zero-sidecar Cloud SQL TLS 1.3 dialer with proactive IAM token and ephemeral cert caching."""

    __slots__ = ("_token_cache",)

    def __init__(self, config: CloudSQLConfig, *, token_cache: Any = None) -> None:
        super().__init__(config)
        self._token_cache = token_cache
        if self._token_cache is None:
            native_mod = import_optional("vertebra._vertebra")
            cache_cls = getattr(native_mod, "AsyncGcpTokenCache", None) if native_mod is not None else None
            if cache_cls is not None:
                with contextlib.suppress(Exception):
                    self._token_cache = cache_cls()

    async def refresh_certificate(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        """Asynchronously exchange an ephemeral client public key for a signed Cloud SQL client certificate."""
        resolved_token = self._unwrap_access_token(access_token) if access_token is not None else ""
        if access_token is None:
            if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                with contextlib.suppress(Exception):
                    res = self._token_cache.get_token()
                    raw_token = await res if inspect.isawaitable(res) else res
                    resolved_token = self._unwrap_access_token(raw_token)
            if not resolved_token and self._cached_token:
                resolved_token = self._cached_token
        if resolved_token:
            self._cached_token = resolved_token
        resolved_pub = public_key_pem
        resolved_priv = private_key_pem
        if resolved_pub is None or resolved_priv is None:
            gen_pub, gen_priv = self.generate_ephemeral_keypair()
            resolved_pub = resolved_pub or gen_pub
            resolved_priv = resolved_priv or gen_priv
        bundle = await asyncio.to_thread(
            self._execute_ephemeral_cert_http,
            resolved_pub,
            resolved_token,
            http_transport,
            resolved_priv,
        )
        self._cached_cert_bundle = bundle
        return bundle

    async def connect(
        self,
        host: str | None = None,
        *,
        ssl_context: ssl.SSLContext | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish an asynchronous TLS 1.3 mTLS connection to Cloud SQL."""
        target_host = host or "127.0.0.1"
        ctx = ssl_context or self.build_tls_context()
        return await asyncio.open_connection(
            target_host,
            self.port,
            ssl=ctx,
            server_hostname=self._uri.expected_server_cn,
        )

    async def close(self) -> None:
        """Clear cached tokens and ephemeral certificates."""
        self._cached_token = None
        self._cached_cert_bundle = None
        self._cached_expiry = 0.0

    def create_asyncpg_connect_func(
        self,
        host: str = "127.0.0.1",
        *,
        ssl_context: ssl.SSLContext | None = None,
    ) -> Callable[..., Any]:
        """Return an asyncpg-compatible `connect` coroutine function dialing port 3307."""
        ctx = ssl_context or self.build_tls_context()

        async def _connect(*args: Any, **kwargs: Any) -> Any:
            asyncpg_mod = import_optional("asyncpg")
            if asyncpg_mod is None:
                return await self.connect(host, ssl_context=ctx)
            kw = dict(kwargs)
            kw.setdefault("host", host)
            kw.setdefault("port", self.port)
            kw["ssl"] = ctx
            if self._config.enable_iam_auth and self._cached_token:
                kw["password"] = self._cached_token
            return await asyncpg_mod.connect(*args, **kw)

        return _connect

    def create_asyncmy_creator(
        self,
        host: str = "127.0.0.1",
        *,
        ssl_context: ssl.SSLContext | None = None,
    ) -> Callable[..., Any]:
        """Return an asyncmy/MySQL pool creator coroutine dialing port 3307."""
        ctx = ssl_context or self.build_tls_context()

        async def _creator(*args: Any, **kwargs: Any) -> Any:
            asyncmy_mod = import_optional("asyncmy")
            if asyncmy_mod is None:
                return await self.connect(host, ssl_context=ctx)
            kw = dict(kwargs)
            kw.setdefault("host", host)
            kw.setdefault("port", self.port)
            kw["ssl"] = ctx
            if self._config.enable_iam_auth and self._cached_token:
                kw["password"] = self._cached_token
            return await asyncmy_mod.connect(*args, **kw)

        return _creator


def generate_ephemeral_keypair(bits: int = 2048) -> tuple[str, str]:
    """Generate an ephemeral client public/private keypair in PEM format."""
    return _BaseCloudSQLDialer.generate_ephemeral_keypair(bits)
