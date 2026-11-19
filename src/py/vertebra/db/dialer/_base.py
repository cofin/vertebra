"""Shared zero-sidecar GCP mTLS dialer primitives for Cloud SQL and AlloyDB."""

from __future__ import annotations

import inspect
import os
import ssl
import tempfile
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Final

import anyio

from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional

VALID_GCP_IP_TYPES: Final[frozenset[str]] = frozenset({"PUBLIC", "PRIVATE", "PSC"})

__all__ = (
    "VALID_GCP_IP_TYPES",
    "EphemeralCertBundle",
    "build_verify_full_tls_context",
    "format_dialer_url_host",
    "generate_ephemeral_keypair",
    "validate_dialer_non_empty",
    "validate_dialer_port",
)


def validate_dialer_port(port: int) -> int:
    """Validate that a TCP port is a non-boolean integer within 1..65535."""
    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
        msg = f"port must be an integer between 1 and 65535, got {port!r}"
        raise ImproperConfigurationError(msg)
    return port


def validate_dialer_non_empty(value: str, *, field_name: str) -> str:
    """Validate that a string configuration field is non-empty and free of NUL bytes."""
    if "\x00" in value:
        msg = f"{field_name} must not contain NUL bytes"
        raise ImproperConfigurationError(msg)
    stripped = value.strip()
    if not stripped:
        msg = f"{field_name} must be a non-empty string"
        raise ImproperConfigurationError(msg)
    return stripped


def format_dialer_url_host(host: str) -> str:
    """Format a hostname or IPv6 literal for safe inclusion in a database URI authority."""
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        return f"[{host}]"
    return host


def build_verify_full_tls_context(ca_pem: str | None = None) -> ssl.SSLContext:
    """Build a `verify-full` client `SSLContext` enforcing TLS 1.2+, `CERT_REQUIRED`, and hostname checks."""
    if ca_pem is not None:
        validated_pem = validate_dialer_non_empty(ca_pem, field_name="ca_pem")
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cadata=validated_pem)
    else:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    return ctx


class EphemeralCertBundle(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Ephemeral mTLS certificate bundle and server endpoint metadata."""

    ca_cert_pem: str
    """PEM-encoded instance CA certificate returned by the control-plane Admin API."""
    client_cert_pem: str
    """PEM-encoded ephemeral client certificate signed by the instance CA."""
    private_key_pem: str
    """PEM-encoded ephemeral client RSA/ECDSA private key."""
    server_ip: str = "127.0.0.1"
    """Resolved public, private, or PSC IP address of the database instance."""
    expiration_ts: float = 0.0
    """Unix epoch timestamp when the ephemeral client certificate expires."""


def generate_ephemeral_keypair(bits: int = 2048) -> tuple[str, str]:
    """Generate an ephemeral client public/private RSA keypair in PEM format."""
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None and bits == 2048:
        native_cls = getattr(native_mod, "CloudSQLDialer", None)
        if native_cls is not None and hasattr(native_cls, "generate_ephemeral_keypair"):
            result = native_cls("p:r:i").generate_ephemeral_keypair()
            if isinstance(result, tuple) and len(result) == 2 and "PUBLIC KEY" in str(result[0]):
                return str(result[0]), str(result[1])

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

    if native_mod is None:
        ensure_module("vertebra._vertebra", package="vertebra-core")
    ensure_module("cryptography.hazmat.primitives.asymmetric.rsa", package="cryptography", extra="cryptography")
    msg = "Ephemeral RSA keypair generation requires vertebra._vertebra or cryptography"
    raise RuntimeError(msg)


class _BaseGCPDialer:
    """Shared TLS 1.3 context construction, IAM token caching, and Admin HTTP helpers."""

    _tls_temp_prefix: ClassVar[str] = "vtb-gcp-tls-"

    __slots__ = (
        "_cached_cert_bundle",
        "_cached_expiry",
        "_cached_token",
        "_config",
        "_token_cache",
        "_uri",
    )

    def __init__(
        self,
        config: Any,
        uri: Any,
        *,
        token_cache: Any = None,
        default_cache_cls_name: str | None = None,
    ) -> None:
        self._config = config
        self._uri = uri
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0
        self._cached_cert_bundle: EphemeralCertBundle | None = None
        self._token_cache = token_cache
        if self._token_cache is None and default_cache_cls_name is not None:
            native_mod = import_optional("vertebra._vertebra")
            cache_cls = getattr(native_mod, default_cache_cls_name, None) if native_mod is not None else None
            if cache_cls is not None:
                self._token_cache = cache_cls()

    @property
    def config(self) -> Any:
        """Return the configuration for this dialer."""
        return self._config

    @property
    def uri(self) -> Any:
        """Return the parsed GCP instance URI."""
        return self._uri

    @property
    def port(self) -> int:
        """Return the remote mTLS proxy port."""
        return int(self._config.port)

    @staticmethod
    def generate_ephemeral_keypair(bits: int = 2048) -> tuple[str, str]:
        """Generate an ephemeral client public/private keypair in PEM format."""
        return generate_ephemeral_keypair(bits)

    def needs_refresh(self, now_ts: float | None = None) -> bool:
        """Return True if the cached IAM token or certificate is within the refresh lead window."""
        current = time.time() if now_ts is None else now_ts
        lead_seconds = int(getattr(self._config, "refresh_lead_seconds", 0))
        return self._cached_token is None or (self._cached_expiry - current) <= lead_seconds

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
        """Build a peer-verified TLS 1.3 ``SSLContext`` for zero-sidecar mTLS connections."""
        if ca_pem:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.load_verify_locations(cadata=ca_pem)
        else:
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        if client_cert_pem and client_key_pem:
            with tempfile.TemporaryDirectory(prefix=self._tls_temp_prefix) as tmpdir:
                os.chmod(tmpdir, 0o700)
                cert_file = Path(tmpdir) / "cert.pem"
                key_file = Path(tmpdir) / "key.pem"
                cert_file.write_text(client_cert_pem, encoding="utf-8")
                key_file.write_text(client_key_pem, encoding="utf-8")
                os.chmod(cert_file, 0o600)
                os.chmod(key_file, 0o600)
                ctx.load_cert_chain(certfile=str(cert_file), keyfile=str(key_file))
        return ctx

    @staticmethod
    def _unwrap_access_token(raw: Any) -> str:
        if raw is None:
            return ""
        if hasattr(raw, "access_token"):
            return str(raw.access_token or "")
        return str(raw or "")

    @staticmethod
    def _post_urllib(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            return bytes(resp.read())

    def _clear_cached_credentials(self) -> None:
        self._cached_token = None
        self._cached_cert_bundle = None
        self._cached_expiry = 0.0

    def _resolve_ssl_context(self, ssl_context: ssl.SSLContext | None) -> ssl.SSLContext:
        if ssl_context is not None:
            return ssl_context
        bundle = self._cached_cert_bundle
        if bundle is not None:
            return self.build_tls_context(
                ca_pem=bundle.ca_cert_pem or None,
                client_cert_pem=bundle.client_cert_pem or None,
                client_key_pem=bundle.private_key_pem or None,
            )
        return self.build_tls_context()

    def _resolve_sync_token(
        self,
        explicit_token: str | None = None,
        *,
        use_cache: bool = True,
    ) -> str:
        resolved = self._unwrap_access_token(explicit_token) if explicit_token is not None else ""
        if explicit_token is None and use_cache:
            if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                resolved = self._unwrap_access_token(self._token_cache.get_token())
            if not resolved and self._cached_token:
                resolved = self._cached_token
        if resolved:
            self._cached_token = resolved
        return resolved

    async def _resolve_async_token(
        self,
        explicit_token: str | None = None,
        *,
        use_cache: bool = True,
    ) -> str:
        resolved = self._unwrap_access_token(explicit_token) if explicit_token is not None else ""
        if explicit_token is None and use_cache:
            if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                res = self._token_cache.get_token()
                raw_token = await res if inspect.isawaitable(res) else res
                resolved = self._unwrap_access_token(raw_token)
            if not resolved and self._cached_token:
                resolved = self._cached_token
        if resolved:
            self._cached_token = resolved
        return resolved

    def _execute_http_sync(
        self,
        url: str,
        payload: bytes,
        headers: dict[str, str],
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> bytes:
        if http_transport is not None:
            return http_transport(url, payload, headers)
        return self._post_urllib(url, payload, headers)

    async def _execute_http_async(
        self,
        url: str,
        payload: bytes,
        headers: dict[str, str],
        http_transport: Callable[[str, bytes, dict[str, str]], Any] | None = None,
    ) -> bytes:
        if http_transport is None:
            return await anyio.to_thread.run_sync(self._post_urllib, url, payload, headers)
        if inspect.iscoroutinefunction(http_transport):
            return bytes(await http_transport(url, payload, headers))
        res = await anyio.to_thread.run_sync(http_transport, url, payload, headers)
        awaited = await res if inspect.isawaitable(res) else res
        return bytes(awaited)

    def _refresh_bundle_sync(
        self,
        build_request: Callable[[str, str], tuple[str, bytes, dict[str, str]]],
        parse_response: Callable[[bytes, str], EphemeralCertBundle],
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        resolved_token = self._resolve_sync_token(access_token)
        resolved_pub = public_key_pem
        resolved_priv = private_key_pem
        if resolved_pub is None or resolved_priv is None:
            gen_pub, gen_priv = self.generate_ephemeral_keypair()
            resolved_pub = resolved_pub or gen_pub
            resolved_priv = resolved_priv or gen_priv
        url, payload, headers = build_request(resolved_pub, resolved_token)
        resp_bytes = self._execute_http_sync(url, payload, headers, http_transport)
        bundle = parse_response(resp_bytes, resolved_priv)
        self._cached_cert_bundle = bundle
        return bundle

    async def _refresh_bundle_async(
        self,
        build_request: Callable[[str, str], tuple[str, bytes, dict[str, str]]],
        parse_response: Callable[[bytes, str], EphemeralCertBundle],
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], Any] | None = None,
    ) -> EphemeralCertBundle:
        resolved_token = await self._resolve_async_token(access_token)
        resolved_pub = public_key_pem
        resolved_priv = private_key_pem
        if resolved_pub is None or resolved_priv is None:
            gen_pub, gen_priv = await anyio.to_thread.run_sync(self.generate_ephemeral_keypair)
            resolved_pub = resolved_pub or gen_pub
            resolved_priv = resolved_priv or gen_priv
        url, payload, headers = build_request(resolved_pub, resolved_token)
        resp_bytes = await self._execute_http_async(url, payload, headers, http_transport)
        bundle = parse_response(resp_bytes, resolved_priv)
        self._cached_cert_bundle = bundle
        return bundle
