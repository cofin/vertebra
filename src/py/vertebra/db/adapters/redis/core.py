"""Redis/Valkey RESP3 wire codecs, CRC16-XMODEM hash slot calculator, and cluster redirect parser."""

from __future__ import annotations

import contextlib
from typing import Any, Literal
from urllib.parse import urlparse

from vertebra.typing import FrozenBaseStruct, VertebraError
from vertebra.utils.module_loader import import_optional

__all__ = (
    "HASH_SLOT_COUNT",
    "NON_KEY_COMMANDS",
    "ClusterRedirect",
    "PubSubMessage",
    "RedisCore",
    "Resp3Error",
    "Resp3Parser",
    "crc16_xmodem",
    "encode_hello3_handshake",
    "encode_resp_command",
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
    """Parsed Redis/Valkey Cluster MOVED or ASK redirection directive."""

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


class Resp3Error(VertebraError):
    """Exception raised when a Redis or Valkey server returns a RESP3 error frame."""

    def __init__(self, message: str, *, next_offset: int = 0) -> None:
        super().__init__(message)
        self.next_offset = next_offset


class Resp3Parser:
    """Zero-copy and low-overhead parser for the Redis Serialization Protocol (RESP3)."""

    @classmethod
    def parse_frame(
        cls,
        data: bytes | bytearray | memoryview,
        offset: int = 0,
    ) -> tuple[Any, int]:
        """Parse a single RESP3 frame from data at offset, returning (value, next_offset)."""
        if isinstance(data, memoryview):
            return cls._parse_view(data, offset)
        buf = memoryview(data)
        try:
            return cls._parse_view(buf, offset)
        finally:
            buf.release()

    @classmethod
    def _parse_view(cls, buf: memoryview, offset: int) -> tuple[Any, int]:
        """Parse a single RESP3 frame from an active memoryview."""
        total_len = len(buf)
        if offset >= total_len:
            msg = "Unexpected end of RESP3 buffer"
            raise ValueError(msg)

        marker = buf[offset]
        pos = offset + 1

        if marker == 43:
            idx = cls._find_crlf(buf, pos)
            text = bytes(buf[pos:idx]).decode("utf-8")
            return text, idx + 2

        if marker == 45:
            idx = cls._find_crlf(buf, pos)
            err_msg = bytes(buf[pos:idx]).decode("utf-8")
            raise Resp3Error(err_msg, next_offset=idx + 2)

        if marker == 33:
            idx = cls._find_crlf(buf, pos)
            length = int(bytes(buf[pos:idx]).decode("ascii"))
            val_start = idx + 2
            val_end = val_start + length
            if val_end + 2 > total_len:
                msg = "Incomplete blob error frame"
                raise ValueError(msg)
            err_msg = bytes(buf[val_start:val_end]).decode("utf-8")
            raise Resp3Error(err_msg, next_offset=val_end + 2)

        if marker == 58:
            idx = cls._find_crlf(buf, pos)
            num = int(bytes(buf[pos:idx]).decode("ascii"))
            return num, idx + 2

        if marker == 44:
            idx = cls._find_crlf(buf, pos)
            val_str = bytes(buf[pos:idx]).decode("ascii").lower()
            if val_str == "inf":
                f_val = float("inf")
            elif val_str == "-inf":
                f_val = float("-inf")
            elif val_str == "nan":
                f_val = float("nan")
            else:
                f_val = float(val_str)
            return f_val, idx + 2

        if marker == 35:
            idx = cls._find_crlf(buf, pos)
            b_char = buf[pos]
            return b_char == 116, idx + 2

        if marker == 95:
            idx = cls._find_crlf(buf, pos)
            return None, idx + 2

        if marker == 36:
            idx = cls._find_crlf(buf, pos)
            length = int(bytes(buf[pos:idx]).decode("ascii"))
            if length == -1:
                return None, idx + 2
            val_start = idx + 2
            val_end = val_start + length
            if val_end + 2 > total_len:
                msg = "Incomplete bulk string frame"
                raise ValueError(msg)
            return bytes(buf[val_start:val_end]), val_end + 2

        if marker == 61:
            idx = cls._find_crlf(buf, pos)
            length = int(bytes(buf[pos:idx]).decode("ascii"))
            val_start = idx + 2
            val_end = val_start + length
            if val_end + 2 > total_len:
                msg = "Incomplete verbatim string frame"
                raise ValueError(msg)
            return bytes(buf[val_start + 4 : val_end]), val_end + 2

        if marker == 42:
            idx = cls._find_crlf(buf, pos)
            count = int(bytes(buf[pos:idx]).decode("ascii"))
            if count == -1:
                return None, idx + 2
            curr = idx + 2
            items: list[Any] = []
            for _ in range(count):
                item, curr = cls._parse_view(buf, curr)
                items.append(item)
            return items, curr

        if marker == 126:
            idx = cls._find_crlf(buf, pos)
            count = int(bytes(buf[pos:idx]).decode("ascii"))
            curr = idx + 2
            set_items: set[Any] = set()
            for _ in range(count):
                item, curr = cls._parse_view(buf, curr)
                set_items.add(item)
            return set_items, curr

        if marker == 37:
            idx = cls._find_crlf(buf, pos)
            pairs = int(bytes(buf[pos:idx]).decode("ascii"))
            curr = idx + 2
            dict_items: dict[Any, Any] = {}
            for _ in range(pairs):
                k, curr = cls._parse_view(buf, curr)
                v, curr = cls._parse_view(buf, curr)
                dict_items[k] = v
            return dict_items, curr

        if marker == 62:
            idx = cls._find_crlf(buf, pos)
            count = int(bytes(buf[pos:idx]).decode("ascii"))
            curr = idx + 2
            push_items: list[Any] = []
            for _ in range(count):
                item, curr = cls._parse_view(buf, curr)
                push_items.append(item)
            return ("push", push_items), curr

        msg = f"Unsupported RESP3 marker byte: {chr(marker)!r} ({marker})"
        raise ValueError(msg)

    @staticmethod
    def _find_crlf(buf: memoryview, start: int) -> int:
        """Locate next carriage return and line feed delimiter."""
        total = len(buf)
        pos = start
        while pos + 1 < total:
            if buf[pos] == 13 and buf[pos + 1] == 10:
                return pos
            pos += 1
        msg = "Incomplete RESP3 frame delimiter"
        raise ValueError(msg)


class RedisCore:
    """RESP3 command encoding, CRC16-XMODEM hash slots, and cluster redirection parsing."""

    @staticmethod
    def crc16_xmodem(data: bytes | bytearray | memoryview | str) -> int:
        """Compute CRC16-XMODEM (polynomial 0x1021, initial value 0x0000) for Redis Cluster."""
        raw = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        native = import_optional("vertebra._vertebra")
        if native is not None:
            native_fn = getattr(native, "crc16_xmodem", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError):
                    return int(native_fn(raw))
        crc = 0x0000
        for byte in raw:
            crc ^= byte << 8
            for _ in range(8):
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF if (crc & 0x8000) else (crc << 1) & 0xFFFF
        return crc

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the Redis Cluster hash slot (0..16383) for key with `{tag}` extraction."""
        raw = key.encode("utf-8") if isinstance(key, str) else bytes(key)
        native = import_optional("vertebra._vertebra")
        if native is not None:
            native_fn = getattr(native, "redis_hash_slot", None) or getattr(native, "redis_hash_slot_native", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError):
                    return int(native_fn(raw))
        start = raw.find(b"{")
        if start != -1:
            end = raw.find(b"}", start + 1)
            if end != -1 and end > start + 1:
                raw = raw[start + 1 : end]
        return RedisCore.crc16_xmodem(raw) % HASH_SLOT_COUNT

    @staticmethod
    def parse_cluster_redirect(error_message: str | bytes) -> ClusterRedirect | None:
        """Parse a Redis Cluster MOVED or ASK redirection response into ClusterRedirect."""
        text = error_message.decode("utf-8") if isinstance(error_message, bytes) else str(error_message)
        native = import_optional("vertebra._vertebra")
        if native is not None:
            native_fn = getattr(native, "parse_cluster_redirect", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError, ValueError):
                    res = native_fn(text)
                    if res is None:
                        return None
                    if isinstance(res, ClusterRedirect):
                        return res
                    if isinstance(res, tuple) and len(res) == 4:
                        kind_str = str(res[0]).upper()
                        kind_lit: Literal["MOVED", "ASK"] = "MOVED" if kind_str == "MOVED" else "ASK"
                        return ClusterRedirect(kind=kind_lit, slot=int(res[1]), host=str(res[2]), port=int(res[3]))
                    if hasattr(res, "kind") and hasattr(res, "slot"):
                        kind_attr: Literal["MOVED", "ASK"] = "MOVED" if str(res.kind).upper() == "MOVED" else "ASK"
                        return ClusterRedirect(
                            kind=kind_attr,
                            slot=int(res.slot),
                            host=str(res.host),
                            port=int(res.port),
                        )
        cleaned = text.strip()
        if cleaned.startswith("-"):
            cleaned = cleaned[1:].strip()
        parts = cleaned.split()
        if len(parts) < 3:
            return None
        directive = parts[0].upper()
        if directive not in ("MOVED", "ASK"):
            return None
        try:
            slot = int(parts[1])
            host_port = parts[2]
            host, port_str = host_port.rsplit(":", 1)
            host = host.strip("[]")
            port = int(port_str)
        except ValueError:
            return None
        if not host or port <= 0 or slot < 0 or slot >= HASH_SLOT_COUNT:
            return None
        kind_val: Literal["MOVED", "ASK"] = "MOVED" if directive == "MOVED" else "ASK"
        return ClusterRedirect(kind=kind_val, slot=slot, host=host, port=port)

    @staticmethod
    def encode_resp_command(*parts: str | bytes | int) -> bytes:
        """Encode a Redis or Valkey RESP3 bulk-string array command."""
        native = import_optional("vertebra._vertebra")
        if native is not None:
            native_fn = getattr(native, "encode_resp_command", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError):
                    encoded = native_fn([RedisCore.encode_part_bytes(p) for p in parts])
                    if isinstance(encoded, bytes):
                        return encoded
        chunks = [f"*{len(parts)}\r\n".encode("ascii")]
        for arg in parts:
            raw = RedisCore.encode_part_bytes(arg)
            chunks.append(f"${len(raw)}\r\n".encode("ascii"))
            chunks.append(raw)
            chunks.append(b"\r\n")
        return b"".join(chunks)

    @staticmethod
    def encode_hello3_handshake(
        *,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = "vertebra",
    ) -> bytes:
        """Encode a RESP3 HELLO 3 handshake command with optional AUTH and SETNAME."""
        native = import_optional("vertebra._vertebra")
        if native is not None:
            native_fn = getattr(native, "encode_hello3_handshake", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError):
                    encoded = bytes(native_fn(client_name or "", username, password, False, []))
                    return encoded
        parts: list[bytes | str] = ["HELLO", "3"]
        if password is not None:
            parts.extend(["AUTH", username or "default", password])
        if client_name:
            parts.extend(["SETNAME", client_name])
        return RedisCore.encode_resp_command(*parts)

    @staticmethod
    def decode_part(part: str | bytes | int) -> str:
        """Decode a command or frame part to string."""
        if isinstance(part, bytes):
            return part.decode("utf-8")
        return str(part)

    @staticmethod
    def encode_part_bytes(part: str | bytes | int) -> bytes:
        """Encode a command part to raw bytes."""
        if isinstance(part, int):
            return str(part).encode("ascii")
        if isinstance(part, str):
            return part.encode("utf-8")
        return bytes(part)

    @staticmethod
    def ensure_bytes(value: Any) -> bytes:
        """Convert a frame payload to bytes."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        return str(value).encode("utf-8")

    @staticmethod
    def normalize_node(node: tuple[str, int] | str, *, default_port: int = 6379) -> tuple[str, int]:
        """Normalize a node endpoint from either `(host, port)` or `'host:port'` string."""
        if isinstance(node, tuple):
            return (str(node[0]), int(node[1]))
        raw = str(node).strip()
        if "://" in raw:
            parsed = urlparse(raw)
            return (parsed.hostname or "127.0.0.1", parsed.port or default_port)
        if ":" in raw:
            host, port_s = raw.rsplit(":", 1)
            return (host.strip("[]"), int(port_s))
        return (raw, default_port)


crc16_xmodem = RedisCore.crc16_xmodem
hash_slot = RedisCore.hash_slot
redis_hash_slot = RedisCore.hash_slot
parse_cluster_redirect = RedisCore.parse_cluster_redirect
encode_resp_command = RedisCore.encode_resp_command
encode_hello3_handshake = RedisCore.encode_hello3_handshake
