"""Shared RESP3 cluster structs, exception types, CRC16-XMODEM hash slots, and redirect parsers."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Final, Literal
from urllib.parse import urlparse

from vertebra._vertebra import DatabaseError
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional

__all__ = (
    "HASH_SLOT_COUNT",
    "NON_KEY_COMMANDS",
    "ClusterRedirect",
    "PubSubMessage",
    "Resp3Error",
    "crc16_xmodem",
    "hash_slot",
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
