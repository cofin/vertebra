"""Type stubs for the native pure-Rust `vertebra._valkey` Valkey 8.0+/9.0 PyO3 module."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from types import TracebackType
from typing import Any, Final, Self, final

__all__ = [
    "FREE_THREADED",
    "GIL_USED",
    "AsyncValkeyClient",
    "AsyncValkeyListener",
    "AsyncValkeyPipeline",
    "SyncValkeyClient",
    "SyncValkeyListener",
    "SyncValkeyPipeline",
    "ValkeyClientConfig",
    "__version__",
    "crc16_xmodem",
    "parse_cluster_redirect",
    "valkey_hash_slot",
]

__version__: Final[str]
FREE_THREADED: Final[bool]
GIL_USED: Final[bool]

def crc16_xmodem(data: bytes) -> int:
    """Compute CRC-16/XMODEM checksum matching Valkey Cluster slot hashing."""

def valkey_hash_slot(key: bytes) -> int:
    """Compute the 14-bit Valkey Cluster hash slot (`0..16384`) respecting `{hash_tag}`."""

def parse_cluster_redirect(message: str) -> tuple[str, int, str, int] | None:
    """Parse a Valkey `MOVED` or `ASK` redirection message into `(kind, slot, host, port)`."""

@final
class ValkeyClientConfig:
    """Validated Valkey connection, Rustls TLS/mTLS, GCP IAM, and topology configuration."""

    def __new__(
        cls,
        url: str | None = None,
        *,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        tls: bool | None = None,
        tls_ca_pem: str | None = None,
        tls_cert_pem: str | None = None,
        tls_key_pem: str | None = None,
        tls_server_name: str | None = None,
        tls_insecure: bool = False,
        unix_socket_path: str | None = None,
        pool_min_size: int = 1,
        pool_max_size: int = 16,
        connect_timeout_ms: int = 5000,
        command_timeout_ms: int = 5000,
        max_redirections: int = 8,
        cluster_enabled: bool = False,
        cluster_nodes: Sequence[str] | None = None,
        read_from_replicas: bool = False,
        sentinel_service_name: str | None = None,
        sentinel_nodes: Sequence[str] | None = None,
        sentinel_username: str | None = None,
        sentinel_password: str | None = None,
        client_tracking_enabled: bool = False,
        client_tracking_prefixes: Sequence[str] | None = None,
        client_tracking_capacity: int = 4096,
        client_tracking_ttl_ms: int | None = None,
        client_name: str | None = None,
        enable_gcp_memorystore_iam: bool = False,
        iam_auth_enabled: bool | None = None,
        token_provider: Any | None = None,
        iam_token_provider: Any | None = None,
        protocol_resp3: bool = True,
        enable_capa_redirect: bool = True,
    ) -> ValkeyClientConfig:
        """Construct a validated `ValkeyClientConfig` instance."""

    @staticmethod
    def from_url(url: str) -> ValkeyClientConfig:
        """Construct a `ValkeyClientConfig` directly from a Valkey connection URL."""

    @property
    def url(self) -> str:
        """Redacted Valkey endpoint URL."""

    @property
    def host(self) -> str:
        """Target hostname."""

    @property
    def port(self) -> int:
        """Target TCP port."""

    @property
    def db(self) -> int:
        """Logical database index."""

    @property
    def username(self) -> str | None:
        """Configured ACL username."""

    @property
    def tls(self) -> bool:
        """Whether TLS transport is active."""

    @property
    def unix_socket_path(self) -> str | None:
        """Unix domain socket path when `unix://` is active."""

    @property
    def cluster_enabled(self) -> bool:
        """Whether Valkey Cluster routing is enabled."""

    @property
    def read_from_replicas(self) -> bool:
        """Whether read-only commands route to cluster replicas."""

    @property
    def sentinel_service_name(self) -> str | None:
        """Configured Sentinel primary service name."""

    @property
    def client_tracking_enabled(self) -> bool:
        """Whether RESP3 `CLIENT TRACKING` L1 caching is active."""

    @property
    def client_tracking_prefixes(self) -> list[str]:
        """Configured `CLIENT TRACKING` `PREFIX` filters."""

    @property
    def enable_gcp_memorystore_iam(self) -> bool:
        """Whether Google Cloud Memorystore for Valkey IAM auth is active."""

    @property
    def max_redirections(self) -> int:
        """Maximum Cluster `MOVED`/`ASK` redirections per command."""

    @property
    def connect_timeout_ms(self) -> int:
        """TCP/TLS dial timeout in milliseconds."""

    @property
    def command_timeout_ms(self) -> int:
        """RESP3 command read/write timeout in milliseconds."""

    @property
    def pool_min_size(self) -> int:
        """Minimum connection pool hint."""

    @property
    def pool_max_size(self) -> int:
        """Maximum idle connections per node bucket."""

    def __repr__(self) -> str:
        """Return redacted diagnostic representation."""

@final
class SyncValkeyListener:
    """Synchronous Valkey Pub/Sub (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) listener."""

    @property
    def closed(self) -> bool:
        """Whether the listener has been closed."""

    def subscribe(self, *channels: str | Sequence[str]) -> None:
        """Subscribe to exact Pub/Sub channels."""

    def psubscribe(self, *patterns: str | Sequence[str]) -> None:
        """Subscribe to glob Pub/Sub channel patterns."""

    def ssubscribe(self, *channels: str | Sequence[str]) -> None:
        """Subscribe to shard-scoped Pub/Sub channels."""

    def unsubscribe(self, *channels: str | Sequence[str]) -> None:
        """Unsubscribe from exact Pub/Sub channels."""

    def punsubscribe(self, *patterns: str | Sequence[str]) -> None:
        """Unsubscribe from glob Pub/Sub channel patterns."""

    def sunsubscribe(self, *channels: str | Sequence[str]) -> None:
        """Unsubscribe from shard-scoped Pub/Sub channels."""

    def next_message(self, timeout_ms: int | None = None) -> dict[str, Any] | None:
        """Wait for the next Pub/Sub message dictionary, returning `None` on timeout."""

    def drain_messages(self, max_messages: int = 1024) -> list[dict[str, Any]]:
        """Drain already-buffered Pub/Sub messages without blocking."""

    def close(self) -> None:
        """Close the listener connection and stop background reader."""

    def __enter__(self) -> Self:
        """Enter synchronous context manager."""

    def __exit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: TracebackType | None = None,
    ) -> bool:
        """Close listener on synchronous context exit."""

@final
class AsyncValkeyListener:
    """Asynchronous Valkey Pub/Sub (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) listener."""

    @property
    def closed(self) -> bool:
        """Whether the listener has been closed."""

    def subscribe(self, *channels: str | Sequence[str]) -> Awaitable[None]:
        """Subscribe to exact Pub/Sub channels."""

    def psubscribe(self, *patterns: str | Sequence[str]) -> Awaitable[None]:
        """Subscribe to glob Pub/Sub channel patterns."""

    def ssubscribe(self, *channels: str | Sequence[str]) -> Awaitable[None]:
        """Subscribe to shard-scoped Pub/Sub channels."""

    def unsubscribe(self, *channels: str | Sequence[str]) -> Awaitable[None]:
        """Unsubscribe from exact Pub/Sub channels."""

    def punsubscribe(self, *patterns: str | Sequence[str]) -> Awaitable[None]:
        """Unsubscribe from glob Pub/Sub channel patterns."""

    def sunsubscribe(self, *channels: str | Sequence[str]) -> Awaitable[None]:
        """Unsubscribe from shard-scoped Pub/Sub channels."""

    def next_message(self, timeout_ms: int | None = None) -> Awaitable[dict[str, Any] | None]:
        """Await the next Pub/Sub message dictionary, returning `None` on timeout."""

    def drain_messages(self, max_messages: int = 1024) -> list[dict[str, Any]]:
        """Drain already-buffered Pub/Sub messages without awaiting."""

    def close(self) -> Awaitable[None]:
        """Close the listener connection and stop background reader."""

    def aclose(self) -> Awaitable[None]:
        """Async alias for `close()`."""

    def __aenter__(self) -> Awaitable[Self]:
        """Enter asynchronous context manager."""

    def __aexit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: TracebackType | None = None,
    ) -> Awaitable[bool]:
        """Close listener on asynchronous context exit."""

@final
class SyncValkeyPipeline:
    """Synchronous batched Valkey pipeline (`SyncValkeyClient.pipeline()`)."""

    @property
    def transaction(self) -> bool:
        """Whether commands are wrapped in `MULTI` / `EXEC`."""

    def __len__(self) -> int:
        """Number of queued commands."""

    def clear(self) -> Self:
        """Discard all queued commands."""

    def get(self, key: bytes | str) -> Self:
        """Queue `GET key`."""

    def set(
        self,
        key: bytes | str,
        value: bytes | str,
        *,
        px: int | None = None,
        ex: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Self:
        """Queue `SET key value`."""

    def delete(self, *keys: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `DEL key ...`."""

    def unlink(self, *keys: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `UNLINK key ...`."""

    def exists(self, *keys: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `EXISTS key ...`."""

    def expire(self, key: bytes | str, seconds: int) -> Self:
        """Queue `EXPIRE key seconds`."""

    def pexpire(self, key: bytes | str, ms: int) -> Self:
        """Queue `PEXPIRE key ms`."""

    def ttl(self, key: bytes | str) -> Self:
        """Queue `TTL key`."""

    def pttl(self, key: bytes | str) -> Self:
        """Queue `PTTL key`."""

    def incr(self, key: bytes | str, amount: int = 1) -> Self:
        """Queue `INCRBY key amount`."""

    def decr(self, key: bytes | str, amount: int = 1) -> Self:
        """Queue `DECRBY key amount`."""

    def hset(
        self,
        key: bytes | str,
        mapping: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]] | bytes | str,
        value: bytes | str | None = None,
    ) -> Self:
        """Queue `HSET` for a mapping or single `(field, value)` pair."""

    def hget(self, key: bytes | str, field: bytes | str) -> Self:
        """Queue `HGET key field`."""

    def hgetall(self, key: bytes | str) -> Self:
        """Queue `HGETALL key`."""

    def hdel(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `HDEL key field ...`."""

    def hexpire(
        self,
        key: bytes | str,
        ttl_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HEXPIRE`."""

    def hpexpire(
        self,
        key: bytes | str,
        ttl_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HPEXPIRE`."""

    def hexpireat(
        self,
        key: bytes | str,
        unix_time_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HEXPIREAT`."""

    def hpexpireat(
        self,
        key: bytes | str,
        unix_time_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HPEXPIREAT`."""

    def httl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue Valkey 9.0 `HTTL`."""

    def hpttl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue Valkey 9.0 `HPTTL`."""

    def hpersist(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue Valkey 9.0 `HPERSIST`."""

    def publish(self, channel: str, message: bytes | str) -> Self:
        """Queue `PUBLISH channel message`."""

    def xadd(
        self,
        key: bytes | str,
        fields: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
    ) -> Self:
        """Queue `XADD` stream append."""

    def xack(self, key: bytes | str, group: str, *ids: str | Sequence[str]) -> Self:
        """Queue `XACK` acknowledgement."""

    def xdel(self, key: bytes | str, *ids: str | Sequence[str]) -> Self:
        """Queue `XDEL` entry deletion."""

    def zadd(
        self,
        key: bytes | str,
        members: Mapping[bytes | str, float] | Sequence[tuple[bytes | str, float]],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> Self:
        """Queue `ZADD` sorted-set insertion."""

    def eval(
        self,
        script: bytes | str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
    ) -> Self:
        """Queue `EVAL` Lua execution."""

    def evalsha(
        self,
        sha1: str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
    ) -> Self:
        """Queue `EVALSHA` Lua execution."""

    def execute_command(self, *args: bytes | str | int | float) -> Self:
        """Queue arbitrary Valkey command."""

    def execute(self) -> list[Any]:
        """Flush queued pipeline commands and return decoded replies in order."""

    def __enter__(self) -> Self:
        """Enter synchronous context manager."""

    def __exit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: TracebackType | None = None,
    ) -> bool:
        """Discard unexecuted commands on context exit."""

@final
class AsyncValkeyPipeline:
    """Asynchronous batched Valkey pipeline (`AsyncValkeyClient.pipeline()`)."""

    @property
    def transaction(self) -> bool:
        """Whether commands are wrapped in `MULTI` / `EXEC`."""

    def __len__(self) -> int:
        """Number of queued commands."""

    def clear(self) -> Self:
        """Discard all queued commands."""

    def get(self, key: bytes | str) -> Self:
        """Queue `GET key`."""

    def set(
        self,
        key: bytes | str,
        value: bytes | str,
        *,
        px: int | None = None,
        ex: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Self:
        """Queue `SET key value`."""

    def delete(self, *keys: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `DEL key ...`."""

    def unlink(self, *keys: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `UNLINK key ...`."""

    def exists(self, *keys: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `EXISTS key ...`."""

    def expire(self, key: bytes | str, seconds: int) -> Self:
        """Queue `EXPIRE key seconds`."""

    def pexpire(self, key: bytes | str, ms: int) -> Self:
        """Queue `PEXPIRE key ms`."""

    def ttl(self, key: bytes | str) -> Self:
        """Queue `TTL key`."""

    def pttl(self, key: bytes | str) -> Self:
        """Queue `PTTL key`."""

    def incr(self, key: bytes | str, amount: int = 1) -> Self:
        """Queue `INCRBY key amount`."""

    def decr(self, key: bytes | str, amount: int = 1) -> Self:
        """Queue `DECRBY key amount`."""

    def hset(
        self,
        key: bytes | str,
        mapping: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]] | bytes | str,
        value: bytes | str | None = None,
    ) -> Self:
        """Queue `HSET` for a mapping or single `(field, value)` pair."""

    def hget(self, key: bytes | str, field: bytes | str) -> Self:
        """Queue `HGET key field`."""

    def hgetall(self, key: bytes | str) -> Self:
        """Queue `HGETALL key`."""

    def hdel(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue `HDEL key field ...`."""

    def hexpire(
        self,
        key: bytes | str,
        ttl_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HEXPIRE`."""

    def hpexpire(
        self,
        key: bytes | str,
        ttl_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HPEXPIRE`."""

    def hexpireat(
        self,
        key: bytes | str,
        unix_time_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HEXPIREAT`."""

    def hpexpireat(
        self,
        key: bytes | str,
        unix_time_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Self:
        """Queue Valkey 9.0 `HPEXPIREAT`."""

    def httl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue Valkey 9.0 `HTTL`."""

    def hpttl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue Valkey 9.0 `HPTTL`."""

    def hpersist(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Self:
        """Queue Valkey 9.0 `HPERSIST`."""

    def publish(self, channel: str, message: bytes | str) -> Self:
        """Queue `PUBLISH channel message`."""

    def xadd(
        self,
        key: bytes | str,
        fields: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
    ) -> Self:
        """Queue `XADD` stream append."""

    def xack(self, key: bytes | str, group: str, *ids: str | Sequence[str]) -> Self:
        """Queue `XACK` acknowledgement."""

    def xdel(self, key: bytes | str, *ids: str | Sequence[str]) -> Self:
        """Queue `XDEL` entry deletion."""

    def zadd(
        self,
        key: bytes | str,
        members: Mapping[bytes | str, float] | Sequence[tuple[bytes | str, float]],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> Self:
        """Queue `ZADD` sorted-set insertion."""

    def eval(
        self,
        script: bytes | str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
    ) -> Self:
        """Queue `EVAL` Lua execution."""

    def evalsha(
        self,
        sha1: str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
    ) -> Self:
        """Queue `EVALSHA` Lua execution."""

    def execute_command(self, *args: bytes | str | int | float) -> Self:
        """Queue arbitrary Valkey command."""

    def execute(self) -> Awaitable[list[Any]]:
        """Flush queued pipeline commands and await decoded replies in order."""

    def __aenter__(self) -> Awaitable[Self]:
        """Enter asynchronous context manager."""

    def __aexit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: TracebackType | None = None,
    ) -> Awaitable[bool]:
        """Discard unexecuted commands on asynchronous context exit."""

@final
class SyncValkeyClient:
    """Synchronous pure-Rust Valkey 8.0+/9.0 client releasing the GIL across I/O."""

    def __new__(cls, config: ValkeyClientConfig | str | None = None) -> SyncValkeyClient:
        """Construct a synchronous Valkey client from config or URL."""

    @staticmethod
    def from_url(url: str) -> SyncValkeyClient:
        """Construct a synchronous Valkey client directly from a connection URL."""

    @property
    def config(self) -> ValkeyClientConfig:
        """Bound immutable configuration."""

    @property
    def closed(self) -> bool:
        """Whether the client has been closed."""

    @property
    def resp3_enabled(self) -> bool:
        """Whether `HELLO 3` negotiation is enabled."""

    @property
    def tracking_enabled(self) -> bool:
        """Whether L1 client-side tracking cache is active."""

    @property
    def cluster_enabled(self) -> bool:
        """Whether Valkey Cluster topology routing is active."""

    def tracking_stats(self) -> dict[str, int]:
        """Return `{hits, misses, invalidations, entries}` for the L1 cache."""

    def cached_keys(self) -> list[str]:
        """Return all key names currently resident in the L1 client cache."""

    def drain_invalidations(self) -> list[str]:
        """Drain and return all queued invalidation events."""

    def invalidate_keys(self, keys: Sequence[bytes | str] | None = None) -> int:
        """Invalidate specific cached keys or the entire cache if keys is None."""

    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""

    def close(self) -> None:
        """Close all pooled Valkey connections."""

    def __enter__(self) -> Self:
        """Enter synchronous context manager."""

    def __exit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: TracebackType | None = None,
    ) -> bool:
        """Close client connections on synchronous context exit."""

    def ping(self) -> bool:
        """Send `PING` and return `True` on `PONG`."""

    def get(self, key: bytes | str) -> bytes | None:
        """Fetch raw value bytes for `key`, checking L1 tracking cache if active."""

    def mget(self, keys: Sequence[bytes | str]) -> list[bytes | None]:
        """Fetch raw value bytes for multiple keys."""

    def set(
        self,
        key: bytes | str,
        value: bytes | str,
        *,
        px: int | None = None,
        ex: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set `key` to `value` with optional expiration and `NX`/`XX` condition."""

    def delete(self, *keys: bytes | str | Sequence[bytes | str]) -> int:
        """Delete one or more keys via `DEL`."""

    def unlink(self, *keys: bytes | str | Sequence[bytes | str]) -> int:
        """Asynchronously unlink one or more keys via `UNLINK`."""

    def exists(self, *keys: bytes | str | Sequence[bytes | str]) -> int:
        """Count existing keys via `EXISTS`."""

    def expire(self, key: bytes | str, seconds: int) -> bool:
        """Set key TTL in whole seconds."""

    def pexpire(self, key: bytes | str, ms: int) -> bool:
        """Set key TTL in milliseconds."""

    def ttl(self, key: bytes | str) -> int:
        """Return key TTL in seconds (`-1` none, `-2` missing)."""

    def pttl(self, key: bytes | str) -> int:
        """Return key TTL in milliseconds (`-1` none, `-2` missing)."""

    def incr(self, key: bytes | str, amount: int = 1) -> int:
        """Increment integer value stored at `key` by `amount`."""

    def decr(self, key: bytes | str, amount: int = 1) -> int:
        """Decrement integer value stored at `key` by `amount`."""

    def scan(
        self,
        cursor: int = 0,
        *,
        match_pattern: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[bytes]]:
        """Execute `SCAN` iteration and return `(next_cursor, keys)`."""

    def scan_unlink_prefix(self, prefix: bytes | str, *, count: int = 200) -> int:
        """Scan matching keys across primary shards and batch-`UNLINK` them per hash slot."""

    def flushdb(self, *, asynchronous: bool = False) -> bool:
        """Flush active database across all primary shards."""

    def publish(self, channel: str, message: bytes | str) -> int:
        """Publish `message` to Pub/Sub `channel`."""

    def hset(
        self,
        key: bytes | str,
        mapping: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]] | bytes | str,
        value: bytes | str | None = None,
    ) -> int:
        """Set one or more hash fields on `key`."""

    def hget(self, key: bytes | str, field: bytes | str) -> bytes | None:
        """Fetch raw value bytes for hash `field` on `key`."""

    def hgetall(self, key: bytes | str) -> dict[bytes, bytes]:
        """Fetch all hash field/value byte pairs on `key`."""

    def hdel(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> int:
        """Delete one or more hash fields on `key`."""

    def hexpire(
        self,
        key: bytes | str,
        ttl_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> list[int]:
        """Execute Valkey 9.0 `HEXPIRE` per-field expiration in seconds."""

    def hpexpire(
        self,
        key: bytes | str,
        ttl_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> list[int]:
        """Execute Valkey 9.0 `HPEXPIRE` per-field expiration in milliseconds."""

    def hexpireat(
        self,
        key: bytes | str,
        unix_time_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> list[int]:
        """Execute Valkey 9.0 `HEXPIREAT` at an absolute Unix timestamp in seconds."""

    def hpexpireat(
        self,
        key: bytes | str,
        unix_time_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> list[int]:
        """Execute Valkey 9.0 `HPEXPIREAT` at an absolute Unix timestamp in milliseconds."""

    def httl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> list[int]:
        """Query Valkey 9.0 `HTTL` remaining field TTL in seconds."""

    def hpttl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> list[int]:
        """Query Valkey 9.0 `HPTTL` remaining field TTL in milliseconds."""

    def hpersist(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> list[int]:
        """Remove Valkey 9.0 hash-field expirations via `HPERSIST`."""

    def script_load(self, script: bytes | str) -> str:
        """Load Lua script across primary shards and return its SHA-1 digest."""

    def eval(
        self,
        script: bytes | str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
    ) -> Any:
        """Execute Lua script optimistically via `EVALSHA` with `NOSCRIPT -> EVAL` fallback."""

    def evalsha(
        self,
        sha1: str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
        *,
        script: bytes | str | None = None,
    ) -> Any:
        """Execute cached Lua script by `sha1`, falling back to `script` on `NOSCRIPT`."""

    def execute_command(self, *args: bytes | str | int | float) -> Any:
        """Execute an arbitrary Valkey command and decode its RESP3 reply."""

    def xgroup_create(
        self,
        key: bytes | str,
        group: str,
        id: str = "0",
        *,
        mkstream: bool = False,
    ) -> bool:
        """Create a Valkey Stream consumer group via `XGROUP CREATE`."""

    def xadd(
        self,
        key: bytes | str,
        fields: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
    ) -> str:
        """Append an entry to a Valkey Stream via `XADD` and return its assigned ID."""

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[bytes | str, str] | Sequence[tuple[bytes | str, str]],
        *,
        count: int | None = None,
        block_ms: int | None = None,
        noack: bool = False,
    ) -> list[tuple[str, list[tuple[str, dict[str, str | bytes]]]]]:
        """Consume entries from Valkey Streams via `XREADGROUP`."""

    def xack(self, key: bytes | str, group: str, *ids: str | Sequence[str]) -> int:
        """Acknowledge processed stream entry IDs via `XACK`."""

    def xautoclaim(
        self,
        key: bytes | str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start_id: str = "0-0",
        *,
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, list[tuple[str, dict[str, str | bytes]]], list[str]]:
        """Reclaim stalled pending stream entries via `XAUTOCLAIM`."""

    def xrange(
        self,
        key: bytes | str,
        min: str = "-",
        max: str = "+",
        *,
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str | bytes]]]:
        """Inspect stream entries in `[min, max]` via `XRANGE`."""

    def xdel(self, key: bytes | str, *ids: str | Sequence[str]) -> int:
        """Delete stream entries via `XDEL`."""

    def zadd(
        self,
        key: bytes | str,
        members: Mapping[bytes | str, float] | Sequence[tuple[bytes | str, float]],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        """Insert or update scored sorted-set members via `ZADD`."""

    def pipeline(self, *, transaction: bool = False) -> SyncValkeyPipeline:
        """Construct a synchronous pipeline bound to this client."""

    def listener(self) -> SyncValkeyListener:
        """Construct a dedicated synchronous Pub/Sub listener."""

@final
class AsyncValkeyClient:
    """Asynchronous pure-Rust Valkey 8.0+/9.0 client backed by Tokio completion queue."""

    def __new__(cls, config: ValkeyClientConfig | str | None = None) -> AsyncValkeyClient:
        """Construct an asynchronous Valkey client from config or URL."""

    @staticmethod
    def from_url(url: str) -> AsyncValkeyClient:
        """Construct an asynchronous Valkey client directly from a connection URL."""

    @property
    def config(self) -> ValkeyClientConfig:
        """Bound immutable configuration."""

    @property
    def closed(self) -> bool:
        """Whether the client has been closed."""

    @property
    def resp3_enabled(self) -> bool:
        """Whether `HELLO 3` negotiation is enabled."""

    @property
    def tracking_enabled(self) -> bool:
        """Whether L1 client-side tracking cache is active."""

    @property
    def cluster_enabled(self) -> bool:
        """Whether Valkey Cluster topology routing is active."""

    def tracking_stats(self) -> dict[str, int]:
        """Return `{hits, misses, invalidations, entries}` for the L1 cache."""

    def cached_keys(self) -> list[str]:
        """Return all key names currently resident in the L1 client cache."""

    def drain_invalidations(self) -> list[str]:
        """Drain and return all queued invalidation events."""

    def invalidate_keys(self, keys: Sequence[bytes | str] | None = None) -> int:
        """Invalidate specific cached keys or the entire cache if keys is None."""

    def slot_map(self) -> dict[int, tuple[str, int]]:
        """Return the cluster hash-slot to (host, port) mapping table."""

    def close(self) -> Awaitable[None]:
        """Close all pooled Valkey connections."""

    def aclose(self) -> Awaitable[None]:
        """Async alias for `close()`."""

    def __aenter__(self) -> Awaitable[Self]:
        """Enter asynchronous context manager."""

    def __aexit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: TracebackType | None = None,
    ) -> Awaitable[bool]:
        """Close client connections on asynchronous context exit."""

    def ping(self) -> Awaitable[bool]:
        """Send `PING` and await `True` on `PONG`."""

    def get(self, key: bytes | str) -> Awaitable[bytes | None]:
        """Fetch raw value bytes for `key`, checking L1 tracking cache if active."""

    def mget(self, keys: Sequence[bytes | str]) -> Awaitable[list[bytes | None]]:
        """Fetch raw value bytes for multiple keys."""

    def set(
        self,
        key: bytes | str,
        value: bytes | str,
        *,
        px: int | None = None,
        ex: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Awaitable[bool]:
        """Set `key` to `value` with optional expiration and `NX`/`XX` condition."""

    def delete(self, *keys: bytes | str | Sequence[bytes | str]) -> Awaitable[int]:
        """Delete one or more keys via `DEL`."""

    def unlink(self, *keys: bytes | str | Sequence[bytes | str]) -> Awaitable[int]:
        """Asynchronously unlink one or more keys via `UNLINK`."""

    def exists(self, *keys: bytes | str | Sequence[bytes | str]) -> Awaitable[int]:
        """Count existing keys via `EXISTS`."""

    def expire(self, key: bytes | str, seconds: int) -> Awaitable[bool]:
        """Set key TTL in whole seconds."""

    def pexpire(self, key: bytes | str, ms: int) -> Awaitable[bool]:
        """Set key TTL in milliseconds."""

    def ttl(self, key: bytes | str) -> Awaitable[int]:
        """Return key TTL in seconds (`-1` none, `-2` missing)."""

    def pttl(self, key: bytes | str) -> Awaitable[int]:
        """Return key TTL in milliseconds (`-1` none, `-2` missing)."""

    def incr(self, key: bytes | str, amount: int = 1) -> Awaitable[int]:
        """Increment integer value stored at `key` by `amount`."""

    def decr(self, key: bytes | str, amount: int = 1) -> Awaitable[int]:
        """Decrement integer value stored at `key` by `amount`."""

    def scan(
        self,
        cursor: int = 0,
        *,
        match_pattern: str | None = None,
        count: int | None = None,
    ) -> Awaitable[tuple[int, list[bytes]]]:
        """Execute `SCAN` iteration and await `(next_cursor, keys)`."""

    def scan_unlink_prefix(self, prefix: bytes | str, *, count: int = 200) -> Awaitable[int]:
        """Scan matching keys across primary shards and batch-`UNLINK` them per hash slot."""

    def flushdb(self, *, asynchronous: bool = False) -> Awaitable[bool]:
        """Flush active database across all primary shards."""

    def publish(self, channel: str, message: bytes | str) -> Awaitable[int]:
        """Publish `message` to Pub/Sub `channel`."""

    def hset(
        self,
        key: bytes | str,
        mapping: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]] | bytes | str,
        value: bytes | str | None = None,
    ) -> Awaitable[int]:
        """Set one or more hash fields on `key`."""

    def hget(self, key: bytes | str, field: bytes | str) -> Awaitable[bytes | None]:
        """Fetch raw value bytes for hash `field` on `key`."""

    def hgetall(self, key: bytes | str) -> Awaitable[dict[bytes, bytes]]:
        """Fetch all hash field/value byte pairs on `key`."""

    def hdel(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Awaitable[int]:
        """Delete one or more hash fields on `key`."""

    def hexpire(
        self,
        key: bytes | str,
        ttl_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Awaitable[list[int]]:
        """Execute Valkey 9.0 `HEXPIRE` per-field expiration in seconds."""

    def hpexpire(
        self,
        key: bytes | str,
        ttl_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Awaitable[list[int]]:
        """Execute Valkey 9.0 `HPEXPIRE` per-field expiration in milliseconds."""

    def hexpireat(
        self,
        key: bytes | str,
        unix_time_seconds: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Awaitable[list[int]]:
        """Execute Valkey 9.0 `HEXPIREAT` at an absolute Unix timestamp in seconds."""

    def hpexpireat(
        self,
        key: bytes | str,
        unix_time_ms: int,
        *fields: bytes | str | Sequence[bytes | str],
        condition: str | None = None,
    ) -> Awaitable[list[int]]:
        """Execute Valkey 9.0 `HPEXPIREAT` at an absolute Unix timestamp in milliseconds."""

    def httl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Awaitable[list[int]]:
        """Query Valkey 9.0 `HTTL` remaining field TTL in seconds."""

    def hpttl(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Awaitable[list[int]]:
        """Query Valkey 9.0 `HPTTL` remaining field TTL in milliseconds."""

    def hpersist(self, key: bytes | str, *fields: bytes | str | Sequence[bytes | str]) -> Awaitable[list[int]]:
        """Remove Valkey 9.0 hash-field expirations via `HPERSIST`."""

    def script_load(self, script: bytes | str) -> Awaitable[str]:
        """Load Lua script across primary shards and return its SHA-1 digest."""

    def eval(
        self,
        script: bytes | str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
    ) -> Awaitable[Any]:
        """Execute Lua script optimistically via `EVALSHA` with `NOSCRIPT -> EVAL` fallback."""

    def evalsha(
        self,
        sha1: str,
        keys: Sequence[bytes | str] | None = None,
        args: Sequence[bytes | str] | None = None,
        *,
        script: bytes | str | None = None,
    ) -> Awaitable[Any]:
        """Execute cached Lua script by `sha1`, falling back to `script` on `NOSCRIPT`."""

    def execute_command(self, *args: bytes | str | int | float) -> Awaitable[Any]:
        """Execute an arbitrary Valkey command and await its decoded RESP3 reply."""

    def xgroup_create(
        self,
        key: bytes | str,
        group: str,
        id: str = "0",
        *,
        mkstream: bool = False,
    ) -> Awaitable[bool]:
        """Create a Valkey Stream consumer group via `XGROUP CREATE`."""

    def xadd(
        self,
        key: bytes | str,
        fields: Mapping[bytes | str, bytes | str] | Sequence[tuple[bytes | str, bytes | str]],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
    ) -> Awaitable[str]:
        """Append an entry to a Valkey Stream via `XADD` and await its assigned ID."""

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[bytes | str, str] | Sequence[tuple[bytes | str, str]],
        *,
        count: int | None = None,
        block_ms: int | None = None,
        noack: bool = False,
    ) -> Awaitable[list[tuple[str, list[tuple[str, dict[str, str | bytes]]]]]]:
        """Consume entries from Valkey Streams via `XREADGROUP`."""

    def xack(self, key: bytes | str, group: str, *ids: str | Sequence[str]) -> Awaitable[int]:
        """Acknowledge processed stream entry IDs via `XACK`."""

    def xautoclaim(
        self,
        key: bytes | str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start_id: str = "0-0",
        *,
        count: int | None = None,
        justid: bool = False,
    ) -> Awaitable[tuple[str, list[tuple[str, dict[str, str | bytes]]], list[str]]]:
        """Reclaim stalled pending stream entries via `XAUTOCLAIM`."""

    def xrange(
        self,
        key: bytes | str,
        min: str = "-",
        max: str = "+",
        *,
        count: int | None = None,
    ) -> Awaitable[list[tuple[str, dict[str, str | bytes]]]]:
        """Inspect stream entries in `[min, max]` via `XRANGE`."""

    def xdel(self, key: bytes | str, *ids: str | Sequence[str]) -> Awaitable[int]:
        """Delete stream entries via `XDEL`."""

    def zadd(
        self,
        key: bytes | str,
        members: Mapping[bytes | str, float] | Sequence[tuple[bytes | str, float]],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> Awaitable[int]:
        """Insert or update scored sorted-set members via `ZADD`."""

    def pipeline(self, *, transaction: bool = False) -> AsyncValkeyPipeline:
        """Construct an asynchronous pipeline bound to this client."""

    def listener(self) -> AsyncValkeyListener:
        """Construct a dedicated asynchronous Pub/Sub listener."""
