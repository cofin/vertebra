"""Valkey RESP3 wire codecs, CRC16-XMODEM hash slot calculator, and cluster redirect parser."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from vertebra.db.driver._resp import (
    HASH_SLOT_COUNT,
    NON_KEY_COMMANDS,
    ClusterRedirect,
    PubSubMessage,
    Resp3Error,
)
from vertebra.db.driver._resp import (
    crc16_xmodem as _shared_crc16_xmodem,
)
from vertebra.db.driver._resp import (
    hash_slot as _shared_hash_slot,
)
from vertebra.db.driver._resp import (
    parse_cluster_redirect as _shared_parse_cluster_redirect,
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from vertebra.db.adapters.valkey.config import ValkeyConfig

__all__ = (
    "HASH_SLOT_COUNT",
    "NON_KEY_COMMANDS",
    "ClusterRedirect",
    "PubSubMessage",
    "Resp3Error",
    "Resp3Parser",
    "ValkeyCore",
    "crc16_xmodem",
    "encode_hello3_handshake",
    "encode_resp_command",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
    "valkey_hash_slot",
)


def _reject_valkey_url(url: str) -> None:
    """Validate that the given URL is a supported Valkey scheme.

    Args:
        url: URL string to inspect.

    Raises:
        ImproperConfigurationError: When the URL begins with an unsupported scheme.
    """
    lower = url.lower()
    if lower.startswith("memory://"):
        msg = "Valkey driver does not support memory:// URLs"
        raise ImproperConfigurationError(msg)
    if lower.startswith("redis://") or lower.startswith("rediss://"):
        msg = "redis:// URLs are not supported by the Valkey driver; use vertebra.db.adapters.redis instead."
        raise ImproperConfigurationError(msg)


def _build_valkey_client_config(mod: Any, config: ValkeyConfig) -> Any:
    """Construct a native ValkeyClientConfig from a ValkeyConfig instance."""
    sentinel_nodes_list = [
        f"{node[0]}:{node[1]}" if isinstance(node, (tuple, list)) else str(node) for node in config.sentinel_nodes
    ]
    cluster_nodes_list = [
        f"{node[0]}:{node[1]}" if isinstance(node, (tuple, list)) else str(node) for node in config.cluster_nodes
    ]
    return mod.ValkeyClientConfig(
        url=config.url,
        username=config.username,
        password=config.password,
        client_name=config.client_name,
        max_redirections=config.max_redirections,
        client_tracking_enabled=(config.enable_client_side_caching or config.enable_client_tracking),
        client_tracking_prefixes=list(config.client_tracking_prefixes),
        client_tracking_capacity=config.effective_cache_capacity,
        client_tracking_ttl_ms=int(config.client_cache_ttl_seconds * 1000) if config.client_cache_ttl_seconds else None,
        enable_gcp_memorystore_iam=config.effective_iam_enabled,
        token_provider=config.token_provider or config.iam_token_provider,
        cluster_enabled=(config.topology == "cluster" or config.cluster_enabled),
        cluster_nodes=cluster_nodes_list if cluster_nodes_list else None,
        sentinel_service_name=config.effective_sentinel_master_name,
        sentinel_nodes=sentinel_nodes_list if sentinel_nodes_list else None,
        pool_max_size=config.max_connections,
        protocol_resp3=(config.protocol == 3),
        enable_capa_redirect=(config.topology == "cluster" or config.cluster_enabled),
    )


def _decode_pubsub_item(item: dict[str, Any]) -> PubSubMessage:
    """Convert a native listener message dictionary into a PubSubMessage struct."""
    kind = str(item.get("type", "message"))
    ch = str(item.get("channel", ""))
    raw_payload = item.get("data", b"")
    payload = raw_payload if isinstance(raw_payload, bytes) else str(raw_payload).encode("utf-8")
    pat = item.get("pattern")
    pat_str = str(pat) if pat is not None else None
    return PubSubMessage(kind=kind, channel=ch, payload=payload, pattern=pat_str)


class Resp3Parser:
    """Zero-copy and low-overhead parser for the Valkey Serialization Protocol (RESP3)."""

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


class ValkeyCore:
    """RESP3 command encoding, CRC16-XMODEM hash slots, and cluster redirection parsing."""

    @staticmethod
    def crc16_xmodem(data: bytes | bytearray | memoryview | str) -> int:
        """Compute CRC16-XMODEM (polynomial 0x1021, initial value 0x0000) for Valkey Cluster."""
        return _shared_crc16_xmodem(data, native_module="vertebra._valkey")

    @staticmethod
    def hash_slot(key: str | bytes) -> int:
        """Compute the Valkey Cluster hash slot (0..16383) for a given key string or bytes."""
        return _shared_hash_slot(key, native_module="vertebra._valkey")

    @classmethod
    def redis_hash_slot(cls, key: str | bytes) -> int:
        """Alias for hash_slot matching the C-extension export name."""
        return cls.hash_slot(key)

    @classmethod
    def valkey_hash_slot(cls, key: str | bytes) -> int:
        """Alias for hash_slot matching the Valkey C-extension export name."""
        return cls.hash_slot(key)

    @staticmethod
    def encode_resp_command(*parts: str | bytes | int | float) -> bytes:
        """Encode arbitrary command parts into a RESP3 array of bulk strings."""
        out = bytearray(f"*{len(parts)}\r\n".encode("ascii"))
        for part in parts:
            if isinstance(part, bytes):
                raw = part
            elif isinstance(part, str):
                raw = part.encode("utf-8")
            else:
                raw = str(part).encode("ascii")
            out.extend(f"${len(raw)}\r\n".encode("ascii"))
            out.extend(raw)
            out.extend(b"\r\n")
        return bytes(out)

    @staticmethod
    def encode_hello3_handshake(
        *,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = None,
    ) -> bytes:
        """Generate the RESP3 HELLO 3 handshake wire frame with optional AUTH and SETNAME."""
        parts: list[str | bytes | int] = ["HELLO", "3"]
        if password is not None:
            parts.extend(["AUTH", username or "default", password])
        if client_name is not None:
            parts.extend(["SETNAME", client_name])
        return ValkeyCore.encode_resp_command(*parts)

    @staticmethod
    def parse_cluster_redirect(error_message: str | bytes) -> ClusterRedirect | None:
        """Parse a Valkey Cluster MOVED or ASK redirection error."""
        return _shared_parse_cluster_redirect(error_message, native_module="vertebra._valkey")

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
        """Normalize a node endpoint from either (host, port) or 'host:port' string."""
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


crc16_xmodem = ValkeyCore.crc16_xmodem
hash_slot = ValkeyCore.hash_slot
redis_hash_slot = ValkeyCore.redis_hash_slot
valkey_hash_slot = ValkeyCore.valkey_hash_slot
encode_resp_command = ValkeyCore.encode_resp_command
encode_hello3_handshake = ValkeyCore.encode_hello3_handshake
parse_cluster_redirect = ValkeyCore.parse_cluster_redirect
