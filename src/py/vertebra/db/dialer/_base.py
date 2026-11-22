"""Shared zero-sidecar GCP mTLS dialer primitives for Cloud SQL and AlloyDB."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import secrets
import socket
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, ClassVar, Final

import anyio

from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError, MissingDependencyError
from vertebra.utils.module_loader import ensure_module, import_optional

logger = logging.getLogger("vertebra.db.dialer")

VALID_GCP_IP_TYPES: Final[frozenset[str]] = frozenset({"PUBLIC", "PRIVATE", "PSC"})
DEFAULT_CONNECTOR_RETRY_COUNT: Final[int] = 3
DEFAULT_CONNECTOR_RETRY_DELAY: Final[float] = 0.5
DEFAULT_CONNECTOR_RETRY_MAX_DELAY: Final[float] = 5.0

_SYSTEM_RANDOM: Final[secrets.SystemRandom] = secrets.SystemRandom()
_MAX_CONNECTOR_ERROR_DEPTH: Final[int] = 8
_TRANSIENT_HTTP_STATUS_CODES: Final[frozenset[int]] = frozenset({429, 500, 502, 503, 504})
_TRANSIENT_GAI_ERRNOS: Final[frozenset[int]] = frozenset(
    code for name in ("EAI_AGAIN",) if isinstance((code := getattr(socket, name, None)), int)
)
_TRANSIENT_GRPC_STATUS_NAMES: Final[frozenset[str]] = frozenset(
    {
        "ABORTED",
        "DEADLINE_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "UNAVAILABLE",
    }
)
_PERMANENT_GRPC_STATUS_NAMES: Final[frozenset[str]] = frozenset(
    {
        "ALREADY_EXISTS",
        "CANCELLED",
        "DATA_LOSS",
        "FAILED_PRECONDITION",
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "OUT_OF_RANGE",
        "PERMISSION_DENIED",
        "UNAUTHENTICATED",
        "UNIMPLEMENTED",
    }
)
_PERMANENT_API_CORE_ERROR_NAMES: Final[tuple[str, ...]] = (
    "AlreadyExists",
    "BadRequest",
    "Cancelled",
    "Conflict",
    "DataLoss",
    "DuplicateCredentialArgs",
    "FailedPrecondition",
    "Forbidden",
    "InvalidArgument",
    "MethodNotAllowed",
    "MethodNotImplemented",
    "NotFound",
    "OutOfRange",
    "PermissionDenied",
    "PreconditionFailed",
    "Unauthenticated",
    "Unauthorized",
)
_TRANSIENT_API_CORE_ERROR_NAMES: Final[tuple[str, ...]] = (
    "Aborted",
    "BadGateway",
    "DeadlineExceeded",
    "GatewayTimeout",
    "InternalServerError",
    "ResourceExhausted",
    "ServiceUnavailable",
    "TooManyRequests",
)
_PERMANENT_CLOUD_SQL_ERROR_NAMES: Final[tuple[str, ...]] = (
    "AutoIAMAuthNotSupported",
    "CacheClosedError",
    "ClosedConnectorError",
    "CloudSQLIPTypeError",
    "ConnectorLoopError",
    "DnsLoopError",
    "IncompatibleDriverError",
    "PlatformNotSupportedError",
    "TLSVersionError",
)
_PERMANENT_ALLOYDB_ERROR_NAMES: Final[tuple[str, ...]] = (
    "ClosedConnectorError",
    "IPTypeNotFoundError",
)
_PERMANENT_NATIVE_ERROR_NAMES: Final[tuple[str, ...]] = (
    "DataError",
    "GoogleCloudAuthError",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotFoundError",
    "NotSupportedError",
    "ProgrammingError",
)

__all__ = (
    "DEFAULT_CONNECTOR_RETRY_COUNT",
    "DEFAULT_CONNECTOR_RETRY_DELAY",
    "DEFAULT_CONNECTOR_RETRY_MAX_DELAY",
    "VALID_GCP_IP_TYPES",
    "EphemeralCertBundle",
    "build_verify_full_tls_context",
    "calculate_connector_backoff_seconds",
    "format_dialer_url_host",
    "generate_ephemeral_keypair",
    "is_transient_connector_error",
    "parse_connector_retry_count",
    "parse_connector_retry_delay",
    "validate_connector_retry_config",
    "validate_dialer_non_empty",
    "validate_dialer_port",
)


def validate_connector_retry_config(
    retry_count: Any = DEFAULT_CONNECTOR_RETRY_COUNT,
    retry_delay: Any = DEFAULT_CONNECTOR_RETRY_DELAY,
    retry_max_delay: Any = DEFAULT_CONNECTOR_RETRY_MAX_DELAY,
) -> tuple[int, float, float]:
    """Validate and normalize zero-sidecar connector retry settings.

    Args:
        retry_count: Maximum retry attempts on transient errors (``0`` disables retries).
        retry_delay: Initial backoff delay in seconds before jittered exponential retries.
        retry_max_delay: Upper bound in seconds for any single retry backoff delay.

    Returns:
        Normalized ``(retry_count, retry_delay, retry_max_delay)`` tuple.

    Raises:
        ImproperConfigurationError: If any setting is invalid, boolean, negative, or non-finite,
            or if ``retry_max_delay < retry_delay``.
    """
    if isinstance(retry_count, bool) or not isinstance(retry_count, int) or retry_count < 0:
        msg = "connector_retry_count must be a non-negative integer"
        raise ImproperConfigurationError(msg)

    delays: list[float] = []
    for key, raw_value in (
        ("connector_retry_delay", retry_delay),
        ("connector_retry_max_delay", retry_max_delay),
    ):
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            msg = f"{key} must be a finite non-negative number"
            raise ImproperConfigurationError(msg)
        value = float(raw_value)
        if not math.isfinite(value) or value < 0.0:
            msg = f"{key} must be a finite non-negative number"
            raise ImproperConfigurationError(msg)
        delays.append(value)

    if delays[1] < delays[0]:
        msg = "connector_retry_max_delay must be greater than or equal to connector_retry_delay"
        raise ImproperConfigurationError(msg)

    return retry_count, delays[0], delays[1]


def parse_connector_retry_count(raw_value: str) -> int:
    """Parse a DSN query parameter value for ``connector_retry_count``."""
    stripped = raw_value.strip()
    if not stripped or not stripped.isascii():
        msg = "connector_retry_count must be a non-negative integer"
        raise ImproperConfigurationError(msg)
    try:
        parsed = int(stripped, 10)
    except ValueError as exc:
        msg = "connector_retry_count must be a non-negative integer"
        raise ImproperConfigurationError(msg) from exc
    if parsed < 0:
        msg = "connector_retry_count must be a non-negative integer"
        raise ImproperConfigurationError(msg)
    return parsed


def parse_connector_retry_delay(raw_value: str, *, field_name: str) -> float:
    """Parse a DSN query parameter value for ``connector_retry_delay`` or ``connector_retry_max_delay``."""
    stripped = raw_value.strip()
    try:
        parsed = float(stripped)
    except ValueError as exc:
        msg = f"{field_name} must be a finite non-negative number"
        raise ImproperConfigurationError(msg) from exc
    if not math.isfinite(parsed) or parsed < 0.0:
        msg = f"{field_name} must be a finite non-negative number"
        raise ImproperConfigurationError(msg)
    return parsed


def calculate_connector_backoff_seconds(
    attempt: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
) -> float:
    """Calculate jittered exponential backoff delay in seconds for connector retries.

    Caps the exponential term before applying half-jitter so retries spread across
    ``[capped / 2, capped]`` when competing callers start concurrently.

    Args:
        attempt: Zero-based retry attempt index (``0`` before the first retry).
        base_delay_seconds: Initial delay in seconds before capping and jitter.
        max_delay_seconds: Upper bound in seconds for any single retry delay.

    Returns:
        Non-negative delay in seconds.
    """
    if base_delay_seconds <= 0.0 or max_delay_seconds <= 0.0:
        return 0.0
    exponent = min(max(attempt, 0), 30)
    capped = min(base_delay_seconds * (2**exponent), max_delay_seconds)
    return _SYSTEM_RANDOM.uniform(capped / 2.0, capped)


def is_transient_connector_error(error: BaseException) -> bool:
    """Return whether a Cloud SQL or AlloyDB dialer error is transient and retryable.

    Inspects loaded optional connector, HTTP, and RPC modules via ``sys.modules`` to avoid
    eager imports when those packages are unused. Walks bounded causal chains
    (``cause``, ``raw_error``, ``urllib.error.URLError.reason``, ``__cause__``, and
    unsuppressed ``__context__``) and fails fast on cancellation, TLS/SSL verification,
    authentication, permission, not-found, configuration, and closed-connector errors.

    Args:
        error: Exception raised while refreshing dialer credentials or opening a connection.

    Returns:
        ``True`` if the failure is transient and safe to retry, otherwise ``False``.
    """
    stack: list[BaseException] = [error]
    seen: set[int] = set()
    depth = 0
    default_verdict = False
    while stack and depth < _MAX_CONNECTOR_ERROR_DEPTH:
        current = stack.pop()
        obj_id = id(current)
        if obj_id in seen:
            continue
        seen.add(obj_id)
        depth += 1

        verdict = _classify_connector_error(current)
        if verdict is not None:
            return verdict
        if _is_retryable_connector_wrapper(current):
            default_verdict = True

        next_candidates = _next_connector_error_causes(current)
        stack.extend(candidate for candidate in reversed(next_candidates) if id(candidate) not in seen)
    return default_verdict


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

    _connector_name: ClassVar[str] = "gcp"
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

    def _connector_retry_settings(self) -> tuple[int, float, float]:
        return validate_connector_retry_config(
            getattr(self._config, "connector_retry_count", DEFAULT_CONNECTOR_RETRY_COUNT),
            getattr(self._config, "connector_retry_delay", DEFAULT_CONNECTOR_RETRY_DELAY),
            getattr(self._config, "connector_retry_max_delay", DEFAULT_CONNECTOR_RETRY_MAX_DELAY),
        )

    def _run_with_retry_sync[T](self, operation: str, fn: Callable[[], T]) -> T:
        max_retries, base_delay, max_delay = self._connector_retry_settings()
        attempt = 0
        while True:
            try:
                return fn()
            except Exception as exc:
                if attempt >= max_retries or not is_transient_connector_error(exc):
                    raise
                retry_delay = calculate_connector_backoff_seconds(attempt, base_delay, max_delay)
                logger.warning(
                    "connector.retry",
                    extra={
                        "connector": self._connector_name,
                        "operation": operation,
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "delay_seconds": round(retry_delay, 3),
                        "error_type": type(exc).__name__,
                    },
                )
            if retry_delay > 0.0:
                time.sleep(retry_delay)
            attempt += 1

    async def _run_with_retry_async[T](self, operation: str, fn: Callable[[], Awaitable[T]]) -> T:
        max_retries, base_delay, max_delay = self._connector_retry_settings()
        attempt = 0
        while True:
            try:
                return await fn()
            except Exception as exc:
                if attempt >= max_retries or not is_transient_connector_error(exc):
                    raise
                retry_delay = calculate_connector_backoff_seconds(attempt, base_delay, max_delay)
                logger.warning(
                    "connector.retry",
                    extra={
                        "connector": self._connector_name,
                        "operation": operation,
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "delay_seconds": round(retry_delay, 3),
                        "error_type": type(exc).__name__,
                    },
                )
            if retry_delay > 0.0:
                await anyio.sleep(retry_delay)
            attempt += 1

    def _execute_http_sync(
        self,
        url: str,
        payload: bytes,
        headers: dict[str, str],
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> bytes:
        def _send() -> bytes:
            if http_transport is not None:
                return http_transport(url, payload, headers)
            return self._post_urllib(url, payload, headers)

        return self._run_with_retry_sync("admin_api", _send)

    async def _execute_http_async(
        self,
        url: str,
        payload: bytes,
        headers: dict[str, str],
        http_transport: Callable[[str, bytes, dict[str, str]], Any] | None = None,
    ) -> bytes:
        async def _send() -> bytes:
            if http_transport is None:
                return await anyio.to_thread.run_sync(self._post_urllib, url, payload, headers)
            if inspect.iscoroutinefunction(http_transport):
                return bytes(await http_transport(url, payload, headers))
            res = await anyio.to_thread.run_sync(http_transport, url, payload, headers)
            awaited = await res if inspect.isawaitable(res) else res
            return bytes(awaited)

        return await self._run_with_retry_async("admin_api", _send)

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


def _instance_of_module_attrs(error: BaseException, module: Any, names: tuple[str, ...]) -> bool:
    for name in names:
        candidate = getattr(module, name, None)
        if isinstance(candidate, type) and isinstance(error, candidate):
            return True
    return False


def _extract_grpc_status_name(error: BaseException) -> str | None:
    raw_status: Any = getattr(error, "grpc_status_code", None)
    if raw_status is None:
        code_attr = getattr(error, "code", None)
        if callable(code_attr):
            try:
                raw_status = code_attr()
            except TypeError:
                raw_status = None
    if raw_status is None:
        return None
    status_name = getattr(raw_status, "name", None)
    if isinstance(status_name, str) and status_name:
        return status_name.strip().upper()
    if isinstance(raw_status, str) and raw_status:
        normalized = raw_status.strip().upper()
        if "." in normalized:
            normalized = normalized.rsplit(".", maxsplit=1)[-1]
        return normalized
    return None


def _classify_connector_error(error: BaseException) -> bool | None:
    if not isinstance(error, Exception):
        return False
    if isinstance(
        error,
        (
            ImproperConfigurationError,
            MissingDependencyError,
            ValueError,
            TypeError,
            KeyError,
            PermissionError,
            ssl.SSLError,
        ),
    ):
        return False
    if isinstance(error, urllib.error.HTTPError):
        status = error.code
        return isinstance(status, int) and not isinstance(status, bool) and status in _TRANSIENT_HTTP_STATUS_CODES
    if isinstance(error, (TimeoutError, ConnectionError, asyncio.IncompleteReadError)):
        return True
    if isinstance(error, socket.gaierror):
        return error.errno in _TRANSIENT_GAI_ERRNOS
    if isinstance(error, urllib.error.URLError):
        return None

    native_mod = sys.modules.get("vertebra._vertebra")
    if native_mod is not None and _instance_of_module_attrs(error, native_mod, _PERMANENT_NATIVE_ERROR_NAMES):
        return False

    cloud_sql_exc_mod = sys.modules.get("google.cloud.sql.connector.exceptions")
    if cloud_sql_exc_mod is not None:
        if _instance_of_module_attrs(error, cloud_sql_exc_mod, _PERMANENT_CLOUD_SQL_ERROR_NAMES):
            return False
        if _instance_of_module_attrs(error, cloud_sql_exc_mod, ("ResourceExhaustedError",)):
            return None

    alloydb_exc_mod = sys.modules.get("google.cloud.alloydb.connector.exceptions")
    if alloydb_exc_mod is not None:
        if _instance_of_module_attrs(error, alloydb_exc_mod, _PERMANENT_ALLOYDB_ERROR_NAMES):
            return False
        if _instance_of_module_attrs(error, alloydb_exc_mod, ("ResourceExhaustedError",)):
            return None

    api_core_exc_mod = sys.modules.get("google.api_core.exceptions")
    if api_core_exc_mod is not None:
        if _instance_of_module_attrs(error, api_core_exc_mod, ("RetryError",)):
            return None
        if _instance_of_module_attrs(error, api_core_exc_mod, _TRANSIENT_API_CORE_ERROR_NAMES):
            return True
        if _instance_of_module_attrs(error, api_core_exc_mod, _PERMANENT_API_CORE_ERROR_NAMES):
            return False

    auth_exc_mod = sys.modules.get("google.auth.exceptions")
    if auth_exc_mod is not None:
        if _instance_of_module_attrs(error, auth_exc_mod, ("DefaultCredentialsError", "OAuthError")):
            return False
        if _instance_of_module_attrs(error, auth_exc_mod, ("TransportError",)):
            return True
        if (
            _instance_of_module_attrs(error, auth_exc_mod, ("RefreshError",))
            and getattr(error, "retryable", False) is True
        ):
            return True

    aiohttp_mod = sys.modules.get("aiohttp")
    if aiohttp_mod is not None:
        if _instance_of_module_attrs(
            error,
            aiohttp_mod,
            (
                "ClientSSLError",
                "ClientConnectorCertificateError",
                "ClientConnectorSSLError",
                "ServerFingerprintMismatch",
            ),
        ):
            return False
        if _instance_of_module_attrs(error, aiohttp_mod, ("ClientResponseError",)):
            response_status = getattr(error, "status", None)
            return (
                isinstance(response_status, int)
                and not isinstance(response_status, bool)
                and response_status in _TRANSIENT_HTTP_STATUS_CODES
            )
        if _instance_of_module_attrs(
            error,
            aiohttp_mod,
            ("ServerTimeoutError", "ClientConnectionError", "ServerDisconnectedError", "ClientConnectorError"),
        ):
            return True

    grpc_status_name = _extract_grpc_status_name(error)
    if grpc_status_name is not None:
        if grpc_status_name in _PERMANENT_GRPC_STATUS_NAMES:
            return False
        if grpc_status_name in _TRANSIENT_GRPC_STATUS_NAMES:
            return True

    return None


def _is_retryable_connector_wrapper(error: BaseException) -> bool:
    cloud_sql_exc_mod = sys.modules.get("google.cloud.sql.connector.exceptions")
    if cloud_sql_exc_mod is not None and _instance_of_module_attrs(
        error, cloud_sql_exc_mod, ("ResourceExhaustedError",)
    ):
        return True
    alloydb_exc_mod = sys.modules.get("google.cloud.alloydb.connector.exceptions")
    if alloydb_exc_mod is not None and _instance_of_module_attrs(error, alloydb_exc_mod, ("ResourceExhaustedError",)):
        return True
    api_core_exc_mod = sys.modules.get("google.api_core.exceptions")
    return api_core_exc_mod is not None and _instance_of_module_attrs(error, api_core_exc_mod, ("RetryError",))


def _next_connector_error_causes(error: BaseException) -> list[BaseException]:
    causes: list[BaseException] = []
    if isinstance(error, urllib.error.URLError) and isinstance(error.reason, BaseException):
        causes.append(error.reason)
    retry_cause = getattr(error, "cause", None)
    if isinstance(retry_cause, BaseException):
        causes.append(retry_cause)
    raw_error = getattr(error, "raw_error", None)
    if isinstance(raw_error, BaseException):
        causes.append(raw_error)
    if error.__cause__ is not None:
        causes.append(error.__cause__)
    if error.__context__ is not None and not error.__suppress_context__:
        causes.append(error.__context__)
    return causes
