"""Redis RESP3 wire codecs, CRC16-XMODEM hash slot calculator, and cluster redirect parser."""

from __future__ import annotations

import contextlib
from typing import Literal
from urllib.parse import urlparse

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional

__all__ = (
    "HASH_SLOT_COUNT",
    "NON_KEY_COMMANDS",
    "ClusterRedirect",
    "PubSubMessage",
    "RedisCore",
    "crc16_xmodem",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
)

HASH_SLOT_COUNT = 16384
NON_KEY_COMMANDS = frozenset(
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
    """Parsed Redis Cluster MOVED or ASK redirection directive."""

    kind: Literal["MOVED", "ASK"]
    slot: int
    host: str
    port: int

    @classmethod
    def parse(cls, error_message: str | bytes) -> ClusterRedirect | None:
        """Parse a MOVED or ASK error string into a ClusterRedirect struct."""
        return RedisCore.parse_cluster_redirect(error_message)


class PubSubMessage(FrozenBaseStruct, frozen=True, gc=False):
    """RESP3 multiplexed Pub/Sub push message."""

    kind: str
    channel: str
    payload: bytes
    pattern: str | None = None


class RedisCore:
    """Redis Cluster hash slots and redirection parsing backed by vertebra._redis."""

    @staticmethod
    def crc16_xmodem(data: bytes | bytearray | memoryview | str) -> int:
        """Compute CRC16-XMODEM (polynomial 0x1021, initial value 0x0000) for Redis Cluster."""
        native = import_optional("vertebra._redis")
        if native is not None:
            fn = getattr(native, "crc16_xmodem", None)
            if callable(fn):
                raw = bytes(data) if not isinstance(data, (bytes, str)) else data
                return int(fn(raw))
        raw_b = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        crc = 0x0000
        for byte in raw_b:
            crc ^= byte << 8
            for _ in range(8):
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF if (crc & 0x8000) else (crc << 1) & 0xFFFF
        return crc

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the Redis Cluster hash slot (0..16383) for a given key string or bytes."""
        native = import_optional("vertebra._redis")
        if native is not None:
            fn = getattr(native, "hash_slot", None) or getattr(native, "redis_hash_slot", None)
            if callable(fn):
                return int(fn(key))
        raw = key if isinstance(key, bytes) else key.encode("utf-8")
        start = raw.find(b"{")
        if start != -1:
            end = raw.find(b"}", start + 1)
            if end != -1 and end > start + 1:
                raw = raw[start + 1 : end]
        return RedisCore.crc16_xmodem(raw) % HASH_SLOT_COUNT

    @classmethod
    def redis_hash_slot(cls, key: str | bytes) -> int:
        """Alias for hash_slot matching the C-extension export name."""
        return cls.hash_slot(key)

    @staticmethod
    def parse_cluster_redirect(error_message: str | bytes) -> ClusterRedirect | None:
        """Parse a Redis Cluster MOVED or ASK redirection error."""
        native = import_optional("vertebra._redis")
        if native is not None:
            fn = getattr(native, "parse_cluster_redirect", None)
            if callable(fn):
                parsed = fn(error_message)
                if parsed is not None:
                    parsed_kind, slot, host, port = parsed
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


crc16_xmodem = RedisCore.crc16_xmodem
hash_slot = RedisCore.hash_slot
redis_hash_slot = RedisCore.redis_hash_slot
parse_cluster_redirect = RedisCore.parse_cluster_redirect
