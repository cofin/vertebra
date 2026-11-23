"""Shared RESP3 cluster structs, exception types, CRC16-XMODEM hash slots, and redirect parsers."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any, ClassVar, Final, Literal, Self
from urllib.parse import urlparse

import msgspec

from vertebra._vertebra import DatabaseError
from vertebra.db.driver._cloud_auth import AZURE_REDIS_SCOPE, AzureEntraTokenProvider, DatabaseTokenProvider
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import import_optional

__all__ = (
    "HASH_SLOT_COUNT",
    "NON_KEY_COMMANDS",
    "BaseRESPConfig",
    "ClusterRedirect",
    "PubSubMessage",
    "Resp3Error",
    "crc16_xmodem",
    "hash_slot",
    "is_resp_auth_rejection",
    "parse_cluster_redirect",
)

HASH_SLOT_COUNT: Final[int] = 16384
NON_KEY_COMMANDS: Final[frozenset[str]] = frozenset(
    {
        "ASKING",
        "AUTH",
        "CLIENT",
        "CLUSTER",
        "COMMAND",
        "ECHO",
        "EXEC",
        "FLUSHALL",
        "FLUSHDB",
        "HELLO",
        "INFO",
        "MULTI",
        "PING",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
        "QUIT",
        "READONLY",
        "READWRITE",
        "ROLE",
        "SELECT",
        "SENTINEL",
        "SSUBSCRIBE",
        "SUBSCRIBE",
        "SUNSUBSCRIBE",
        "UNSUBSCRIBE",
    }
)


class ClusterRedirect(FrozenBaseStruct, frozen=True, gc=False):
    """Parsed RESP Cluster MOVED or ASK redirection directive."""

    kind: Literal["MOVED", "ASK"]
    slot: int
    host: str
    port: int

    @classmethod
    def parse(cls, error_message: str | bytes) -> ClusterRedirect | None:
        """Parse a MOVED or ASK error string into a ClusterRedirect struct."""
        return parse_cluster_redirect(error_message)


class PubSubMessage(FrozenBaseStruct, frozen=True, gc=False):
    """RESP3 multiplexed Pub/Sub push message."""

    kind: str
    channel: str
    payload: bytes
    pattern: str | None = None


class Resp3Error(DatabaseError):
    """Exception raised when a RESP3 server returns an error frame."""

    def __init__(self, message: str, *, next_offset: int = 0) -> None:
        super().__init__(message)
        self.next_offset = next_offset


def crc16_xmodem(
    data: bytes | bytearray | memoryview | str,
    *,
    native_module: str | None = None,
) -> int:
    """Compute CRC16-XMODEM (polynomial 0x1021, initial value 0x0000) for RESP cluster keys."""
    raw = data.encode("utf-8") if isinstance(data, str) else bytes(data)
    if native_module is not None:
        native = import_optional(native_module)
        if native is not None:
            native_fn: Callable[[bytes], int] | None = getattr(native, "crc16_xmodem", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError):
                    return int(native_fn(raw))
    crc = 0x0000
    for byte in raw:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if (crc & 0x8000) else (crc << 1) & 0xFFFF
    return crc


def hash_slot(key: str | bytes, *, native_module: str | None = None) -> int:
    """Compute the RESP cluster hash slot (0..16383) for a key string or bytes."""
    raw = key if isinstance(key, bytes) else key.encode("utf-8")
    if native_module is not None:
        native = import_optional(native_module)
        if native is not None:
            native_fn: Callable[[bytes], int] | None = (
                getattr(native, "hash_slot", None)
                or getattr(native, "redis_hash_slot", None)
                or getattr(native, "valkey_hash_slot", None)
            )
            if callable(native_fn):
                with contextlib.suppress(TypeError):
                    return int(native_fn(raw))
    start = raw.find(b"{")
    if start != -1:
        end = raw.find(b"}", start + 1)
        if end != -1 and end > start + 1:
            raw = raw[start + 1 : end]
    return crc16_xmodem(raw, native_module=native_module) % HASH_SLOT_COUNT


def parse_cluster_redirect(
    error_message: str | bytes,
    *,
    native_module: str | None = None,
) -> ClusterRedirect | None:
    """Parse a RESP Cluster MOVED or ASK redirection error."""
    if native_module is not None:
        native = import_optional(native_module)
        if native is not None:
            native_fn: Callable[[str], tuple[str, int, str, int] | None] | None = getattr(
                native, "parse_cluster_redirect", None
            )
            if callable(native_fn):
                msg_str = (
                    error_message.decode("utf-8", errors="replace")
                    if isinstance(error_message, bytes)
                    else str(error_message)
                )
                with contextlib.suppress(TypeError):
                    parsed_native = native_fn(msg_str)
                    if parsed_native is not None:
                        parsed_kind, slot, host, port = parsed_native
                        return ClusterRedirect(
                            kind="MOVED" if parsed_kind == "MOVED" else "ASK",
                            slot=slot,
                            host=host,
                            port=port,
                        )
    text = error_message.decode("utf-8", errors="replace") if isinstance(error_message, bytes) else error_message
    text = text.strip()
    if text.startswith("-"):
        text = text[1:].strip()
    parts = text.split()
    if len(parts) >= 3 and parts[0] in ("MOVED", "ASK"):
        kind: Literal["MOVED", "ASK"] = "MOVED" if parts[0] == "MOVED" else "ASK"
        with contextlib.suppress(ValueError):
            slot = int(parts[1])
            target = parts[2]
            parsed_url = urlparse(f"//{target}")
            if parsed_url.hostname and parsed_url.port:
                return ClusterRedirect(
                    kind=kind,
                    slot=slot,
                    host=parsed_url.hostname,
                    port=parsed_url.port,
                )
    return None


def _encode_resp_array(parts: list[str]) -> bytes:
    encoded = [p.encode("utf-8") for p in parts]
    header = f"*{len(encoded)}\r\n".encode("ascii")
    return header + b"".join(f"${len(item)}\r\n".encode("ascii") + item + b"\r\n" for item in encoded)


def is_resp_auth_rejection(exc: BaseException) -> bool:
    """Return True when ``exc`` represents a RESP authentication rejection."""
    detail = str(exc).upper()
    return any(
        token in detail
        for token in (
            "WRONGPASS",
            "NOAUTH",
            "UNAUTH",
            "ERRAUTH",
            "AUTHENTICATIONFAILED",
            "AUTHENTICATION FAILED",
            "INVALID USERNAME-PASSWORD PAIR",
            "AUTH FAILED",
        )
    )


class BaseRESPConfig(BaseDatabaseConfig, frozen=True, gc=False, kw_only=True):
    """Shared frozen configuration base for Redis and Valkey RESP3 adapters."""

    dialect: ClassVar[str] = "resp"
    url: str = "redis://localhost:6379/0"
    username: str | None = None
    password: str | None = None
    client_name: str = "vertebra"
    protocol: Literal[2, 3] = 3
    topology: Literal["standalone", "sentinel", "cluster"] = "standalone"
    sentinel_service_name: str | None = None
    sentinel_master_name: str | None = None
    sentinel_nodes: tuple[tuple[str, int] | str, ...] = ()
    cluster_enabled: bool = False
    cluster_nodes: tuple[tuple[str, int] | str, ...] = ()
    max_redirections: int = 5
    enable_client_side_caching: bool = False
    enable_client_tracking: bool = False
    client_tracking_prefixes: tuple[str, ...] = ()
    client_cache_capacity: int = 4096
    max_cached_keys: int = 4096
    client_cache_ttl_seconds: float | None = None
    enable_gcp_iam_auth: bool = False
    iam_auth_enabled: bool = False
    token_provider: Any = None
    iam_token_provider: Any = None
    max_connections: int = 32
    broker: Any = None

    def __post_init__(self) -> None:
        cls_name = type(self).__name__
        if (
            self.token_provider is not None
            and self.iam_token_provider is not None
            and self.token_provider is not self.iam_token_provider
        ):
            raise ImproperConfigurationError(
                f"Cannot configure both token_provider and iam_token_provider on {cls_name}"
            )
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        gcp_iam_flag = bool(
            getattr(self, "enable_gcp_memorystore_iam", False) or self.iam_auth_enabled or self.enable_gcp_iam_auth
        )
        if gcp_iam_flag and isinstance(provider, DatabaseTokenProvider):
            raise ImproperConfigurationError(
                f"Cannot combine GCP IAM authentication (enable_gcp_iam_auth=True) with a "
                f"DatabaseTokenProvider on {cls_name}; configure one credential mechanism"
            )
        if isinstance(self.token_provider, AzureEntraTokenProvider):
            msgspec.structs.force_setattr(
                self, "token_provider", self.token_provider.with_default_scope(AZURE_REDIS_SCOPE)
            )
        if isinstance(self.iam_token_provider, AzureEntraTokenProvider):
            msgspec.structs.force_setattr(
                self, "iam_token_provider", self.iam_token_provider.with_default_scope(AZURE_REDIS_SCOPE)
            )

    @staticmethod
    def _redact_url(url: str) -> str:
        """Return a URL representation with embedded userinfo passwords masked."""
        if "://" in url:
            scheme, rest = url.split("://", 1)
            if "@" in rest:
                userinfo, tail = rest.split("@", 1)
                if ":" in userinfo:
                    user, _ = userinfo.split(":", 1)
                    return f"{scheme}://{user}:***@{tail}"
        return url

    @property
    def effective_cache_capacity(self) -> int:
        """Return the configured LRU client cache capacity."""
        if self.client_cache_capacity != 4096:
            return self.client_cache_capacity
        if self.max_cached_keys != 4096:
            return self.max_cached_keys
        return self.client_cache_capacity

    @property
    def effective_iam_enabled(self) -> bool:
        """Return True when GCP Memorystore IAM authentication is enabled."""
        return bool(
            self.iam_auth_enabled
            or self.enable_gcp_iam_auth
            or getattr(self, "enable_gcp_memorystore_iam", False)
            or (self.token_provider is not None)
            or (self.iam_token_provider is not None)
        )

    @property
    def effective_token_provider(self) -> Callable[[], Any] | None:
        """Return the configured IAM token provider callable."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        return provider if callable(provider) else None

    @property
    def effective_database_token_provider(self) -> DatabaseTokenProvider | None:
        """Return the configured `DatabaseTokenProvider` instance when present."""
        provider = self.token_provider if self.token_provider is not None else self.iam_token_provider
        return provider if isinstance(provider, DatabaseTokenProvider) else None

    @property
    def effective_tracking_enabled(self) -> bool:
        """Return True when RESP3 client-side tracking/caching is enabled."""
        return bool(self.enable_client_tracking or self.enable_client_side_caching)

    @property
    def effective_sentinel_master_name(self) -> str | None:
        """Return the configured Sentinel master service name."""
        return self.sentinel_master_name or self.sentinel_service_name

    @property
    def effective_cluster_enabled(self) -> bool:
        """Return True when Cluster topology routing is active."""
        return bool(self.cluster_enabled or self.topology == "cluster" or self.cluster_nodes)

    @property
    def effective_sentinel_enabled(self) -> bool:
        """Return True when Sentinel master discovery is active."""
        return bool(self.topology == "sentinel" or (self.sentinel_nodes and self.effective_sentinel_master_name))

    def _handshake_credentials(self, iam_token: str | None) -> tuple[str | None, str | None]:
        parsed = urlparse(self.url)
        iam_active = self.effective_iam_enabled
        effective_password = (
            iam_token
            if (iam_active and iam_token is not None)
            else (self.password if self.password is not None else parsed.password)
        )
        effective_user = (
            (self.username or parsed.username or "default")
            if iam_active
            else (self.username if self.username is not None else parsed.username)
        )
        return effective_user, effective_password

    def build_handshake_commands(self, *, iam_token: str | None = None) -> list[bytes]:
        """Build the initial RESP3 HELLO 3 and optional CLIENT TRACKING wire commands."""
        effective_user, effective_password = self._handshake_credentials(iam_token)
        parts = ["HELLO", "3"]
        if effective_password is not None:
            parts.extend(["AUTH", effective_user or "default", effective_password])
        if self.client_name:
            parts.extend(["SETNAME", self.client_name])
        cmds = [_encode_resp_array(parts)]
        if self.effective_tracking_enabled:
            track_parts = ["CLIENT", "TRACKING", "ON"]
            for prefix in self.client_tracking_prefixes:
                track_parts.extend(["PREFIX", prefix])
            track_parts.append("BCAST")
            cmds.append(_encode_resp_array(track_parts))
        return cmds

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a config instance from a RESP URI."""
        return cls(url=url, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-pool fingerprint tuple."""
        memorystore_id = getattr(self, "memorystore_instance_id", None) or ""
        default_tracking = self.dialect == "valkey"
        db_provider = self.effective_database_token_provider
        if (
            not self.username
            and self.protocol == 3
            and self.topology == "standalone"
            and not self.effective_sentinel_master_name
            and not self.sentinel_nodes
            and not self.effective_cluster_enabled
            and not self.cluster_nodes
            and self.effective_tracking_enabled is default_tracking
            and not self.client_tracking_prefixes
            and not self.effective_iam_enabled
            and not memorystore_id
            and db_provider is None
        ):
            return (self.dialect, self.url)
        base_key = (
            self.dialect,
            self.url,
            self.username or "",
            self.protocol,
            self.topology,
            self.effective_sentinel_master_name or "",
            self.sentinel_nodes,
            self.effective_cluster_enabled,
            self.cluster_nodes,
            self.effective_tracking_enabled,
            self.client_tracking_prefixes,
            self.effective_iam_enabled,
            memorystore_id,
        )
        if db_provider is None:
            return base_key
        try:
            provider_key: Any = hash(db_provider)
        except TypeError:
            provider_key = id(db_provider)
        return (*base_key, type(db_provider).__qualname__, provider_key)
